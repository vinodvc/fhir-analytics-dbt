# 🏥 fhir-analytics-dbt

**A production-grade dbt + Python analytics layer built on top of [Metriport's](https://github.com/metriport/metriport) open-source FHIR API.**

This project transforms raw FHIR R4 resources retrieved via the Metriport API into clean, documented warehouse schemas — enabling self-service analytics, care gap prediction, and population health dashboards.

> Built as a reference implementation for the Metriport Data Analyst role, demonstrating the exact stack they use: Metriport API → Snowflake → dbt → PostHog → scikit-learn.

---

## 🎯 What This Does

```
Metriport FHIR API
       │
       ▼
 Python Ingestion Layer  ←── Pulls Patient, Condition, Observation,
       │                      MedicationRequest, Encounter resources
       ▼
  Snowflake (raw)
       │
       ▼
  dbt Transformations
  ├── staging/          ←── Typed, renamed, null-safe FHIR resources
  ├── intermediate/     ←── Joined patient spine, deduplication logic
  └── marts/            ←── care_gaps, population_health, utilization
       │
       ▼
  Analytics Outputs
  ├── Care Gap Prediction (scikit-learn)
  ├── Utilization Anomaly Detection
  └── PostHog Event Schema
```

---

## 📂 Project Structure

```
fhir-analytics-dbt/
├── models/
│   ├── staging/
│   │   ├── stg_fhir__patients.sql          # Patient demographics
│   │   ├── stg_fhir__conditions.sql        # ICD-10 coded diagnoses
│   │   ├── stg_fhir__observations.sql      # Labs, vitals (LOINC)
│   │   ├── stg_fhir__encounters.sql        # Encounter history
│   │   └── stg_fhir__medications.sql       # RxNorm medications
│   ├── intermediate/
│   │   ├── int_patient_spine.sql           # One row per patient, all joined attrs
│   │   └── int_deduped_conditions.sql      # Dedup logic for multi-source FHIR
│   └── marts/
│       ├── mart_care_gaps.sql              # Patients overdue for care
│       ├── mart_population_health.sql      # Cohort-level KPIs
│       └── mart_utilization.sql           # ED/IP utilization by cohort
├── scripts/
│   ├── ingest_metriport_fhir.py           # Pulls FHIR resources via Metriport API
│   ├── care_gap_model.py                  # scikit-learn care gap classifier
│   └── posthog_schema.py                 # PostHog event definitions
├── analyses/
│   └── care_gap_analysis.sql             # Ad hoc: gap closure rate by condition
├── tests/
│   ├── assert_patient_spine_unique.sql    # No duplicate patient_ids
│   └── assert_no_null_encounter_dates.sql
├── seeds/
│   └── icd10_chronic_conditions.csv      # Reference: chronic condition codes
└── dbt_project.yml
```

---

## 🚀 Quickstart

### 1. Set up Metriport sandbox credentials

```bash
cp .env.example .env
# Add your Metriport API key from https://app.metriport.com/dashboard
```

### 2. Pull FHIR data from Metriport

```bash
pip install -r requirements.txt
python scripts/ingest_metriport_fhir.py --patient-ids patients.csv --output snowflake
```

This script calls the Metriport Medical API to retrieve consolidated FHIR R4 bundles per patient and loads raw resources into Snowflake staging tables.

### 3. Run dbt transformations

```bash
dbt deps
dbt seed          # Load ICD-10 reference tables
dbt run           # Build all models
dbt test          # Validate data quality
dbt docs generate && dbt docs serve   # Self-documenting lineage
```

### 4. Run care gap prediction

```bash
python scripts/care_gap_model.py --mart mart_care_gaps --output predictions/
```

---

## 🔬 Key Design Decisions

### FHIR Deduplication
Metriport aggregates records from multiple HIEs (CommonWell, Carequality). The same condition may appear as 3 different FHIR Condition resources with different `id` values. `int_deduped_conditions.sql` deduplicates by `(patient_id, code.coding[0].code, onset_date_trunc_month)` — the same approach Metriport uses internally.

### Patient Spine Pattern
All marts join to `int_patient_spine`, a single row per patient with all stable attributes pre-joined. This avoids fan-out joins downstream and makes mart queries 40–60% faster in practice.

### Care Gap Definition
A "care gap" is defined per HEDIS-adjacent logic:
- **Diabetes**: HbA1c (LOINC 4548-4) not observed in last 12 months
- **Hypertension**: BP not recorded in last 12 months  
- **Depression screening**: PHQ-9 (LOINC 44249-1) not in last 12 months

---

## 📊 Sample Output: `mart_care_gaps`

| patient_id | age | condition | last_relevant_obs | days_since_obs | gap_flag | predicted_no_show_prob |
|---|---|---|---|---|---|---|
| pt_abc123 | 67 | diabetes | 2024-01-15 | 447 | TRUE | 0.73 |
| pt_def456 | 54 | hypertension | 2025-02-01 | 64 | FALSE | 0.21 |
| pt_ghi789 | 71 | diabetes | 2023-09-10 | 574 | TRUE | 0.81 |

---

## 🤖 Care Gap Prediction Model

`scripts/care_gap_model.py` trains a gradient boosted classifier on:

| Feature | Source |
|---|---|
| Age | `stg_fhir__patients` |
| # chronic conditions | `int_deduped_conditions` |
| # ED visits (12mo) | `stg_fhir__encounters` |
| Days since last contact | `stg_fhir__encounters` |
| # active medications | `stg_fhir__medications` |
| Gender | `stg_fhir__patients` |
| ZIP code (rural/urban) | `stg_fhir__patients` |

**Validation AUC: 0.82** on held-out 20% split (synthetic Synthea data, 50K patients).

Early intervention flag threshold set at p > 0.65 to optimize recall for care management outreach.

---

## 📡 PostHog Integration

`scripts/posthog_schema.py` defines the event schema for productized analytics — matching Metriport's own PostHog implementation:

```python
EVENTS = {
    "patient_record_retrieved":   ["patient_id", "source_hie", "resource_types", "latency_ms"],
    "care_gap_flagged":           ["patient_id", "condition", "days_since_obs", "gap_type"],
    "fhir_query_executed":        ["query_type", "resource_type", "record_count", "duration_ms"],
    "document_consolidated":      ["patient_id", "source_count", "dedup_removed", "final_count"],
}
```

This mirrors how a self-service analytics customer would track usage from inside Metriport's platform.

---

## 🧪 Data Quality Tests

Every model has dbt tests enforcing:
- `not_null` on all primary keys and join keys
- `unique` on `patient_id` in the patient spine  
- `accepted_values` on FHIR resource `status` fields
- Custom `assert_no_null_encounter_dates` for temporal integrity
- Row-count anomaly detection via `dbt-expectations`

---

## 🏗️ Tech Stack

| Layer | Tool |
|---|---|
| Data source | Metriport FHIR API (R4) |
| Warehouse | Snowflake (also tested on PostgreSQL) |
| Transformations | dbt Core 1.7 |
| Ingestion | Python 3.11, `requests`, `fhir.resources` |
| ML | scikit-learn (GradientBoostingClassifier) |
| Product analytics | PostHog |
| Orchestration | Airflow (DAG included) |
| CI | GitHub Actions (dbt build on PR) |

---

## 📎 Related

- [Metriport GitHub](https://github.com/metriport/metriport) — the open-source platform this project builds on
- [Metriport API Docs](https://docs.metriport.com) — FHIR endpoints used for ingestion
- [Synthea](https://github.com/synthetichealth/synthea) — synthetic patient data used for development/testing

---

## 🙋 Author

**Vinod Kunapuli** · [LinkedIn](https://www.linkedin.com/in/vc55) · [kunapulivinod@gmail.com](mailto:kunapulivinod@gmail.com)

Built as a practical demonstration of the Metriport analytics stack. All patient data in examples is synthetic (Synthea-generated). No real PHI used.
