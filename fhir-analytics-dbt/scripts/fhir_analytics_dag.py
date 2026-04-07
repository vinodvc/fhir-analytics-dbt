"""
fhir_analytics_dag.py
---------------------
Airflow DAG: Daily FHIR ingestion + dbt build + care gap model scoring.

Schedule: Daily at 4:00 AM PST (12:00 UTC)
SLA: All marts available by 7:00 AM PST for the 7:30 AM standup

Pipeline:
  1. [ingest]   Pull updated FHIR bundles from Metriport API → Snowflake raw
  2. [dbt]      Run dbt staging → intermediate → marts
  3. [dbt test] Validate data quality (fail pipeline on test failure)
  4. [ml]       Score care gap model on latest mart_care_gaps
  5. [notify]   Post summary to Slack / PostHog dashboard alert

Designed to mirror the kind of pipeline Metriport's Data Analyst would own.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.models import Variable

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------

default_args = {
    "owner": "vinod.kunapuli",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure": True,
    "email": ["vinod@metriport-analytics.io"],
    "sla": timedelta(hours=3),  # Must complete 3h after 4 AM start = 7 AM
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="fhir_analytics_daily",
    default_args=default_args,
    description="Daily FHIR ingestion + dbt + care gap model scoring",
    schedule_interval="0 12 * * *",   # 4:00 AM PST = 12:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=["fhir", "metriport", "dbt", "clinical"],
) as dag:

    # -----------------------------------------------------------------------
    # Task 1: Pull list of patients updated in Metriport in last 24h
    # -----------------------------------------------------------------------

    def get_updated_patients(**context):
        """
        Query Snowflake for patients whose FHIR data was last updated
        more than 24h ago and need a refresh from the Metriport API.

        In production: Metriport webhooks would push updates to a queue;
        here we poll to find stale records.
        """
        import snowflake.connector
        import os

        conn = snowflake.connector.connect(
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
            database=os.environ["SNOWFLAKE_DATABASE"],
        )
        cur = conn.cursor()
        cur.execute("""
            select patient_id
            from raw_fhir.patients
            where _ingested_at < dateadd('hour', -23, current_timestamp())
               or _ingested_at is null
            limit 5000
        """)
        patient_ids = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()

        # Push to XCom for next task
        context["ti"].xcom_push(key="patient_ids", value=patient_ids)
        print(f"Found {len(patient_ids)} patients needing refresh")
        return len(patient_ids)

    get_stale_patients = PythonOperator(
        task_id="get_stale_patients",
        python_callable=get_updated_patients,
    )

    # -----------------------------------------------------------------------
    # Task 2: Ingest FHIR data from Metriport API
    # -----------------------------------------------------------------------

    def run_ingestion(**context):
        """Pull FHIR bundles for stale patients via the Metriport API."""
        import subprocess
        import json
        import tempfile
        import os

        patient_ids = context["ti"].xcom_pull(
            key="patient_ids", task_ids="get_stale_patients"
        )

        if not patient_ids:
            print("No stale patients — skipping ingestion")
            return 0

        # Write patient list to temp CSV for the ingestion script
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv",
                                        delete=False) as f:
            f.write("patient_id\n")
            f.writelines(f"{pid}\n" for pid in patient_ids)
            temp_path = f.name

        result = subprocess.run(
            ["python", "scripts/ingest_metriport_fhir.py",
             "--patient-ids", temp_path,
             "--output", "snowflake"],
            capture_output=True, text=True, check=True
        )
        os.unlink(temp_path)
        print(result.stdout)
        return len(patient_ids)

    ingest_fhir = PythonOperator(
        task_id="ingest_fhir_from_metriport",
        python_callable=run_ingestion,
        execution_timeout=timedelta(hours=1),
    )

    # -----------------------------------------------------------------------
    # Task 3: dbt run (staging → intermediate → marts)
    # -----------------------------------------------------------------------

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
            cd /opt/airflow/dbt/fhir-analytics-dbt && \
            dbt run \
                --target prod \
                --select staging intermediate marts \
                --vars '{"run_date": "{{ ds }}"}'
        """,
        execution_timeout=timedelta(minutes=45),
    )

    # -----------------------------------------------------------------------
    # Task 4: dbt test (fail pipeline if data quality checks fail)
    # -----------------------------------------------------------------------

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
            cd /opt/airflow/dbt/fhir-analytics-dbt && \
            dbt test \
                --target prod \
                --select staging intermediate marts \
                --store-failures
        """,
        execution_timeout=timedelta(minutes=20),
    )

    # -----------------------------------------------------------------------
    # Task 5: Score care gap model
    # -----------------------------------------------------------------------

    score_care_gaps = BashOperator(
        task_id="score_care_gaps",
        bash_command="""
            python /opt/airflow/scripts/care_gap_model.py \
                --source snowflake \
                --mart mart_care_gaps \
                --output /tmp/care_gap_predictions/{{ ds }}/ \
                --save-model
        """,
        execution_timeout=timedelta(minutes=30),
    )

    # -----------------------------------------------------------------------
    # Task 6: Write predictions back to Snowflake
    # -----------------------------------------------------------------------

    def load_predictions(**context):
        """Load ML predictions CSV back to Snowflake for dashboard use."""
        import pandas as pd
        import snowflake.connector
        import os
        from pathlib import Path

        run_date = context["ds"]
        pred_dir = f"/tmp/care_gap_predictions/{run_date}/"
        pred_files = list(Path(pred_dir).glob("care_gap_predictions_*.csv"))

        if not pred_files:
            raise FileNotFoundError(f"No prediction files found in {pred_dir}")

        df = pd.read_csv(pred_files[0])
        print(f"Loading {len(df):,} predictions to Snowflake")

        conn = snowflake.connector.connect(
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
            database=os.environ["SNOWFLAKE_DATABASE"],
        )
        cur = conn.cursor()

        # Merge predictions into Snowflake (upsert)
        cur.execute(f"""
            create or replace temp table care_gap_predictions_stage as
            select
                $1::varchar as patient_id,
                $2::varchar as condition_type,
                $3::float   as gap_risk_score,
                $4::boolean as high_risk_flag,
                $5::timestamp_ntz as scored_at
            from values {', '.join(str(tuple(r)) for r in df.itertuples(index=False))}
        """)

        cur.execute("""
            merge into analytics.care_gap_ml_scores t
            using care_gap_predictions_stage s
                on t.patient_id = s.patient_id
                and t.condition_type = s.condition_type
            when matched then update set
                gap_risk_score = s.gap_risk_score,
                high_risk_flag = s.high_risk_flag,
                scored_at      = s.scored_at
            when not matched then insert
                (patient_id, condition_type, gap_risk_score, high_risk_flag, scored_at)
            values
                (s.patient_id, s.condition_type, s.gap_risk_score, s.high_risk_flag, s.scored_at)
        """)
        conn.commit()
        cur.close()
        conn.close()
        print("Predictions loaded successfully")

    load_ml_predictions = PythonOperator(
        task_id="load_ml_predictions",
        python_callable=load_predictions,
    )

    # -----------------------------------------------------------------------
    # Task 7: Slack notification with pipeline summary
    # -----------------------------------------------------------------------

    def build_slack_message(**context):
        """Build a Slack summary message with key metrics from today's run."""
        patient_count = context["ti"].xcom_pull(
            key="return_value", task_ids="ingest_fhir_from_metriport"
        ) or 0
        run_date = context["ds"]
        return (
            f":white_check_mark: *FHIR Analytics Pipeline — {run_date}*\n"
            f"• Patients refreshed: `{patient_count:,}`\n"
            f"• dbt models: all green\n"
            f"• Care gap scores: updated\n"
            f"• Marts available: `mart_care_gaps`, `mart_utilization`, `mart_population_health`\n"
            f"_Ready for 7:30 AM standup_ :coffee:"
        )

    slack_notify = SlackWebhookOperator(
        task_id="slack_notify_success",
        slack_webhook_conn_id="slack_analytics",
        message="{{ ti.xcom_pull(task_ids='build_slack_msg') }}",
        trigger_rule="all_success",
    )

    # -----------------------------------------------------------------------
    # DAG dependencies
    # -----------------------------------------------------------------------

    (
        get_stale_patients
        >> ingest_fhir
        >> dbt_run
        >> dbt_test
        >> score_care_gaps
        >> load_ml_predictions
        >> slack_notify
    )
