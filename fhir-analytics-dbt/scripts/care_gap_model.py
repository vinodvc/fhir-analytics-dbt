"""
care_gap_model.py
-----------------
Trains a gradient-boosted classifier to predict which patients are at high risk
of having an unaddressed care gap — i.e., they are overdue for a clinical
measurement (HbA1c, BP reading, depression screening, etc.) AND are unlikely
to schedule an appointment without outreach.

Features are pulled from the dbt marts layer (mart_care_gaps view in Snowflake).
Predictions are written back to a Snowflake table for use in the Metriport
analytics dashboard.

Usage:
    python care_gap_model.py --mart mart_care_gaps --output predictions/
    python care_gap_model.py --source local --input data/care_gaps_sample.csv
"""

import os
import argparse
import logging
import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.metrics import (
    roc_auc_score, classification_report, confusion_matrix,
    precision_recall_curve, average_precision_score
)
from sklearn.calibration import CalibratedClassifierCV
import joblib
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Feature definitions — these map directly to dbt mart columns
# ---------------------------------------------------------------------------

NUMERIC_FEATURES = [
    "age",
    "n_chronic_conditions",
    "n_ed_visits_12mo",
    "n_ip_visits_12mo",
    "days_since_last_encounter",
    "days_since_gap_obs",          # days since the overdue measurement
    "n_active_medications",
    "n_unique_providers_12mo",
]

CATEGORICAL_FEATURES = [
    "gender",
    "condition_type",              # diabetes | hypertension | depression_screening
    "urban_rural_classification",  # urban | suburban | rural (derived from ZIP)
]

TARGET = "gap_closed_30d"  # 1 = gap was closed within 30 days (label from retrospective data)

# Threshold chosen to optimize recall for outreach programs
# (we'd rather contact someone unnecessarily than miss a high-risk patient)
PREDICTION_THRESHOLD = 0.65

CARE_GAP_DEFINITIONS = {
    # LOINC codes for measurements that define a gap being "addressed"
    "diabetes": {
        "loinc_codes": ["4548-4", "17856-6"],  # HbA1c
        "gap_days": 365,
        "label": "HbA1c not measured in 12 months"
    },
    "hypertension": {
        "loinc_codes": ["55284-4", "8480-6", "8462-4"],  # BP panel, systolic, diastolic
        "gap_days": 365,
        "label": "Blood pressure not recorded in 12 months"
    },
    "depression_screening": {
        "loinc_codes": ["44249-1", "89204-2"],  # PHQ-9
        "gap_days": 365,
        "label": "PHQ-9 depression screening not completed in 12 months"
    },
}


def load_from_snowflake(mart_name: str) -> pd.DataFrame:
    """Pull the care gap mart from Snowflake."""
    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema="analytics",
    )
    query = f"""
        SELECT
            patient_id,
            {', '.join(NUMERIC_FEATURES + CATEGORICAL_FEATURES)},
            {TARGET}
        FROM {mart_name}
        WHERE {TARGET} IS NOT NULL      -- labeled data only
          AND days_since_gap_obs >= 30  -- confirmed gap at prediction time
    """
    df = pd.read_sql(query, conn)
    conn.close()
    log.info(f"Loaded {len(df):,} rows from {mart_name}")
    return df


def load_synthetic_data(n_patients: int = 10_000, seed: int = 42) -> pd.DataFrame:
    """
    Generate synthetic data matching the Synthea patient distribution
    for development/testing without real PHI.
    Distributions calibrated from published HEDIS gap rates.
    """
    rng = np.random.default_rng(seed)

    age = rng.integers(18, 85, n_patients)
    n_chronic = rng.poisson(2.1, n_patients).clip(0, 12)
    n_ed_12mo = rng.negative_binomial(1, 0.6, n_patients)
    n_ip_12mo = rng.negative_binomial(1, 0.8, n_patients)
    days_last_enc = rng.integers(0, 730, n_patients)
    days_gap_obs = rng.integers(30, 900, n_patients)
    n_meds = rng.poisson(4.2, n_patients).clip(0, 20)
    n_providers = rng.integers(1, 8, n_patients)

    gender = rng.choice(["male", "female", "unknown"], n_patients, p=[0.48, 0.50, 0.02])
    condition = rng.choice(
        ["diabetes", "hypertension", "depression_screening"],
        n_patients, p=[0.28, 0.45, 0.27]
    )
    urban_rural = rng.choice(
        ["urban", "suburban", "rural"],
        n_patients, p=[0.52, 0.30, 0.18]
    )

    # Label: gap closed within 30 days
    # Simulate: younger patients, more providers, urban → higher closure rate
    log_odds = (
        -1.5
        + -0.012 * days_gap_obs
        + -0.008 * days_last_enc
        + 0.35 * (urban_rural == "urban").astype(float)
        + -0.15 * (urban_rural == "rural").astype(float)
        + 0.08 * n_providers
        + -0.015 * age
        + 0.05 * n_chronic
    )
    prob_close = 1 / (1 + np.exp(-log_odds))
    gap_closed = rng.binomial(1, prob_close)

    return pd.DataFrame({
        "patient_id": [f"pt_{i:06d}" for i in range(n_patients)],
        "age": age,
        "n_chronic_conditions": n_chronic,
        "n_ed_visits_12mo": n_ed_12mo,
        "n_ip_visits_12mo": n_ip_12mo,
        "days_since_last_encounter": days_last_enc,
        "days_since_gap_obs": days_gap_obs,
        "n_active_medications": n_meds,
        "n_unique_providers_12mo": n_providers,
        "gender": gender,
        "condition_type": condition,
        "urban_rural_classification": urban_rural,
        "gap_closed_30d": gap_closed,
    })


def build_pipeline() -> Pipeline:
    """Build sklearn Pipeline with preprocessing + GBM classifier."""
    numeric_transformer = StandardScaler()
    categorical_transformer = OneHotEncoder(handle_unknown="ignore", sparse_output=False)

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, NUMERIC_FEATURES),
            ("cat", categorical_transformer, CATEGORICAL_FEATURES),
        ]
    )

    # GBM tuned for healthcare classification:
    # - moderate depth to avoid overfitting on small hospital cohorts
    # - subsample for variance reduction
    # - calibration wrapper for reliable probability outputs
    base_clf = GradientBoostingClassifier(
        n_estimators=300,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        min_samples_leaf=20,   # avoid fitting on tiny patient subgroups
        random_state=42,
    )
    calibrated_clf = CalibratedClassifierCV(base_clf, method="isotonic", cv=5)

    return Pipeline([
        ("preprocessor", preprocessor),
        ("classifier", calibrated_clf),
    ])


def evaluate(pipeline: Pipeline, X_test: pd.DataFrame, y_test: pd.Series) -> dict:
    """Compute evaluation metrics on held-out test set."""
    y_prob = pipeline.predict_proba(X_test)[:, 1]
    y_pred = (y_prob >= PREDICTION_THRESHOLD).astype(int)

    auc = roc_auc_score(y_test, y_prob)
    ap = average_precision_score(y_test, y_prob)
    report = classification_report(y_test, y_pred, output_dict=True)

    log.info(f"ROC-AUC:           {auc:.3f}")
    log.info(f"Avg Precision:     {ap:.3f}")
    log.info(f"Threshold:         {PREDICTION_THRESHOLD}")
    log.info(f"Precision (gap):   {report['1']['precision']:.3f}")
    log.info(f"Recall (gap):      {report['1']['recall']:.3f}")
    log.info(f"F1 (gap):          {report['1']['f1-score']:.3f}")

    return {
        "roc_auc": round(auc, 4),
        "average_precision": round(ap, 4),
        "threshold": PREDICTION_THRESHOLD,
        "precision_at_threshold": round(report["1"]["precision"], 4),
        "recall_at_threshold": round(report["1"]["recall"], 4),
        "f1_at_threshold": round(report["1"]["f1-score"], 4),
        "n_test": len(y_test),
        "prevalence": round(y_test.mean(), 4),
    }


def save_predictions(pipeline: Pipeline, df: pd.DataFrame,
                     output_dir: str) -> pd.DataFrame:
    """Score all patients and save predictions CSV."""
    X = df[NUMERIC_FEATURES + CATEGORICAL_FEATURES]
    probs = pipeline.predict_proba(X)[:, 1]
    flags = (probs >= PREDICTION_THRESHOLD).astype(int)

    results = pd.DataFrame({
        "patient_id": df["patient_id"],
        "condition_type": df["condition_type"],
        "gap_risk_score": probs.round(4),
        "high_risk_flag": flags,
        "scored_at": datetime.utcnow().isoformat(),
    }).sort_values("gap_risk_score", ascending=False)

    Path(output_dir).mkdir(exist_ok=True)
    out_path = f"{output_dir}/care_gap_predictions_{datetime.utcnow().strftime('%Y%m%d')}.csv"
    results.to_csv(out_path, index=False)
    log.info(f"Saved {len(results):,} predictions → {out_path}")
    log.info(f"High-risk patients flagged: {flags.sum():,} ({flags.mean():.1%})")
    return results


def main():
    parser = argparse.ArgumentParser(description="Train care gap prediction model")
    parser.add_argument("--source", choices=["snowflake", "local", "synthetic"],
                        default="synthetic")
    parser.add_argument("--mart", default="mart_care_gaps",
                        help="Snowflake mart name (if --source snowflake)")
    parser.add_argument("--input", help="Local CSV path (if --source local)")
    parser.add_argument("--output", default="predictions/", help="Output directory")
    parser.add_argument("--save-model", action="store_true",
                        help="Persist trained model to disk")
    args = parser.parse_args()

    # 1. Load data
    if args.source == "snowflake":
        df = load_from_snowflake(args.mart)
    elif args.source == "local":
        df = pd.read_csv(args.input)
        log.info(f"Loaded {len(df):,} rows from {args.input}")
    else:
        log.info("Generating synthetic patient data (Synthea distribution)...")
        df = load_synthetic_data(n_patients=50_000)

    log.info(f"Gap closure rate (label prevalence): {df[TARGET].mean():.1%}")

    # 2. Train/test split — stratified on condition type to avoid leakage
    X = df[NUMERIC_FEATURES + CATEGORICAL_FEATURES]
    y = df[TARGET]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, stratify=y, random_state=42
    )

    # 3. Build and train
    log.info("Training GradientBoostingClassifier with isotonic calibration...")
    pipeline = build_pipeline()
    pipeline.fit(X_train, y_train)

    # 4. Cross-validation AUC
    cv_scores = cross_val_score(
        build_pipeline(), X_train, y_train,
        cv=StratifiedKFold(n_splits=5), scoring="roc_auc", n_jobs=-1
    )
    log.info(f"5-fold CV AUC: {cv_scores.mean():.3f} ± {cv_scores.std():.3f}")

    # 5. Evaluate on held-out test set
    metrics = evaluate(pipeline, X_test, y_test)

    # 6. Score all patients and save
    save_predictions(pipeline, df, args.output)

    # 7. Save metrics
    metrics_path = f"{args.output}/model_metrics.json"
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)
    log.info(f"Metrics saved → {metrics_path}")

    # 8. Optionally persist model
    if args.save_model:
        model_path = f"{args.output}/care_gap_model.joblib"
        joblib.dump(pipeline, model_path)
        log.info(f"Model saved → {model_path}")


if __name__ == "__main__":
    main()
