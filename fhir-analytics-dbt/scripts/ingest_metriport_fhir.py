"""
ingest_metriport_fhir.py
------------------------
Pulls consolidated FHIR R4 bundles from the Metriport Medical API
and loads raw resources into Snowflake staging tables.

Metriport API docs: https://docs.metriport.com/medical-api/api-reference
"""

import os
import json
import time
import logging
import argparse
from datetime import datetime
from typing import Optional

import requests
import pandas as pd
import snowflake.connector
from fhir.resources.bundle import Bundle
from fhir.resources.patient import Patient
from fhir.resources.condition import Condition
from fhir.resources.observation import Observation
from fhir.resources.encounter import Encounter
from fhir.resources.medicationrequest import MedicationRequest
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

METRIPORT_BASE_URL = "https://api.metriport.com/medical/v1"
RESOURCE_TYPES = ["Patient", "Condition", "Observation", "Encounter", "MedicationRequest"]


class MetriportFHIRClient:
    """
    Thin wrapper around the Metriport Medical API.
    Handles auth, retries, and rate limiting.
    """

    def __init__(self, api_key: str, facility_id: str):
        self.api_key = api_key
        self.facility_id = facility_id
        self.session = requests.Session()
        self.session.headers.update({
            "x-api-key": self.api_key,
            "Content-Type": "application/json",
        })

    def _get(self, path: str, params: dict = None, retries: int = 3) -> dict:
        url = f"{METRIPORT_BASE_URL}{path}"
        for attempt in range(retries):
            try:
                resp = self.session.get(url, params=params, timeout=30)
                resp.raise_for_status()
                return resp.json()
            except requests.HTTPError as e:
                if resp.status_code == 429:
                    wait = 2 ** attempt
                    log.warning(f"Rate limited, waiting {wait}s (attempt {attempt+1})")
                    time.sleep(wait)
                else:
                    raise
        raise RuntimeError(f"Failed after {retries} retries: {url}")

    def get_patient(self, patient_id: str) -> dict:
        """GET /patient/{id}"""
        return self._get(f"/patient/{patient_id}")

    def start_consolidated_query(self, patient_id: str,
                                  resources: list[str] = None,
                                  date_from: str = None,
                                  date_to: str = None) -> str:
        """
        POST /patient/{id}/consolidated/query
        Returns a query_id to poll for completion.
        Metriport aggregates across all connected HIEs (CommonWell, Carequality).
        """
        body = {
            "resources": resources or RESOURCE_TYPES,
        }
        if date_from:
            body["dateFrom"] = date_from
        if date_to:
            body["dateTo"] = date_to

        resp = self.session.post(
            f"{METRIPORT_BASE_URL}/patient/{patient_id}/consolidated/query",
            json=body,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        log.info(f"Started consolidated query for patient {patient_id}: status={data.get('status')}")
        return data

    def get_consolidated_query_status(self, patient_id: str) -> dict:
        """GET /patient/{id}/consolidated/query — poll until status=completed"""
        return self._get(f"/patient/{patient_id}/consolidated/query")

    def get_consolidated_bundle(self, patient_id: str) -> dict:
        """GET /patient/{id}/consolidated — returns full FHIR Bundle"""
        return self._get(f"/patient/{patient_id}/consolidated")

    def wait_for_consolidated(self, patient_id: str,
                               poll_interval: int = 5,
                               max_wait: int = 120) -> dict:
        """
        Starts a consolidated query and polls until complete.
        Metriport queries can take 10–60s depending on HIE response times.
        """
        self.start_consolidated_query(patient_id)
        elapsed = 0
        while elapsed < max_wait:
            status_resp = self.get_consolidated_query_status(patient_id)
            status = status_resp.get("status")
            log.info(f"Patient {patient_id} query status: {status} ({elapsed}s elapsed)")
            if status == "completed":
                return self.get_consolidated_bundle(patient_id)
            elif status == "failed":
                raise RuntimeError(f"Consolidated query failed for patient {patient_id}")
            time.sleep(poll_interval)
            elapsed += poll_interval
        raise TimeoutError(f"Timed out waiting for patient {patient_id} after {max_wait}s")


def parse_fhir_bundle(bundle_json: dict) -> dict[str, list[dict]]:
    """
    Parse a FHIR R4 Bundle into typed resource lists.
    Returns dict keyed by resourceType.

    Metriport returns consolidated bundles that may have duplicate resources
    from multiple HIE sources — we preserve raw and deduplicate downstream in dbt.
    """
    resources: dict[str, list[dict]] = {rt: [] for rt in RESOURCE_TYPES}
    entries = bundle_json.get("entry", [])

    for entry in entries:
        resource = entry.get("resource", {})
        rt = resource.get("resourceType")
        if rt in resources:
            resources[rt].append(resource)

    counts = {rt: len(v) for rt, v in resources.items() if v}
    log.info(f"Parsed bundle: {counts}")
    return resources


def flatten_patient(r: dict) -> dict:
    """Flatten FHIR Patient resource to a tabular row."""
    name = r.get("name", [{}])[0]
    address = r.get("address", [{}])[0]
    return {
        "patient_id":     r.get("id"),
        "family_name":    name.get("family"),
        "given_name":     " ".join(name.get("given", [])),
        "birth_date":     r.get("birthDate"),
        "gender":         r.get("gender"),
        "postal_code":    address.get("postalCode"),
        "city":           address.get("city"),
        "state":          address.get("state"),
        "active":         r.get("active", True),
        "_ingested_at":   datetime.utcnow().isoformat(),
        "_raw":           json.dumps(r),
    }


def flatten_condition(r: dict) -> dict:
    """Flatten FHIR Condition resource."""
    coding = r.get("code", {}).get("coding", [{}])[0]
    subject = r.get("subject", {}).get("reference", "").replace("Patient/", "")
    onset = r.get("onsetDateTime") or r.get("onsetPeriod", {}).get("start")
    return {
        "condition_id":       r.get("id"),
        "patient_id":         subject,
        "clinical_status":    r.get("clinicalStatus", {}).get("coding", [{}])[0].get("code"),
        "verification_status":r.get("verificationStatus", {}).get("coding", [{}])[0].get("code"),
        "code_system":        coding.get("system"),
        "code":               coding.get("code"),
        "display":            coding.get("display"),
        "onset_datetime":     onset,
        "recorded_date":      r.get("recordedDate"),
        "_ingested_at":       datetime.utcnow().isoformat(),
        "_raw":               json.dumps(r),
    }


def flatten_observation(r: dict) -> dict:
    """Flatten FHIR Observation resource (labs, vitals)."""
    coding = r.get("code", {}).get("coding", [{}])[0]
    subject = r.get("subject", {}).get("reference", "").replace("Patient/", "")
    value_qty = r.get("valueQuantity", {})
    value_code = r.get("valueCodeableConcept", {}).get("coding", [{}])[0]
    return {
        "observation_id":     r.get("id"),
        "patient_id":         subject,
        "status":             r.get("status"),
        "loinc_code":         coding.get("code") if "loinc" in (coding.get("system") or "") else None,
        "display":            coding.get("display"),
        "effective_datetime": r.get("effectiveDateTime"),
        "value_quantity":     value_qty.get("value"),
        "value_unit":         value_qty.get("unit"),
        "value_code":         value_code.get("code"),
        "value_display":      value_code.get("display"),
        "interpretation":     r.get("interpretation", [{}])[0].get("coding", [{}])[0].get("code"),
        "_ingested_at":       datetime.utcnow().isoformat(),
        "_raw":               json.dumps(r),
    }


def flatten_encounter(r: dict) -> dict:
    """Flatten FHIR Encounter resource."""
    subject = r.get("subject", {}).get("reference", "").replace("Patient/", "")
    period = r.get("period", {})
    type_coding = r.get("type", [{}])[0].get("coding", [{}])[0]
    class_coding = r.get("class", {})
    return {
        "encounter_id":       r.get("id"),
        "patient_id":         subject,
        "status":             r.get("status"),
        "class_code":         class_coding.get("code"),   # IMP, AMB, EMER
        "class_display":      class_coding.get("display"),
        "type_code":          type_coding.get("code"),
        "type_display":       type_coding.get("display"),
        "period_start":       period.get("start"),
        "period_end":         period.get("end"),
        "_ingested_at":       datetime.utcnow().isoformat(),
        "_raw":               json.dumps(r),
    }


def flatten_medication_request(r: dict) -> dict:
    """Flatten FHIR MedicationRequest resource."""
    subject = r.get("subject", {}).get("reference", "").replace("Patient/", "")
    med = r.get("medicationCodeableConcept", {})
    rxnorm = next((c for c in med.get("coding", [])
                   if "rxnorm" in (c.get("system") or "").lower()), {})
    return {
        "medication_request_id": r.get("id"),
        "patient_id":            subject,
        "status":                r.get("status"),
        "intent":                r.get("intent"),
        "rxnorm_code":           rxnorm.get("code"),
        "medication_display":    rxnorm.get("display") or med.get("text"),
        "authored_on":           r.get("authoredOn"),
        "dosage_text":           r.get("dosageInstruction", [{}])[0].get("text"),
        "_ingested_at":          datetime.utcnow().isoformat(),
        "_raw":                  json.dumps(r),
    }


FLATTENERS = {
    "Patient":           flatten_patient,
    "Condition":         flatten_condition,
    "Observation":       flatten_observation,
    "Encounter":         flatten_encounter,
    "MedicationRequest": flatten_medication_request,
}

SNOWFLAKE_TABLES = {
    "Patient":           "raw_fhir.patients",
    "Condition":         "raw_fhir.conditions",
    "Observation":       "raw_fhir.observations",
    "Encounter":         "raw_fhir.encounters",
    "MedicationRequest": "raw_fhir.medication_requests",
}


def load_to_snowflake(records: dict[str, list[dict]], conn: snowflake.connector.SnowflakeConnection):
    """Write flattened records to Snowflake raw tables via batch insert."""
    cur = conn.cursor()
    for resource_type, rows in records.items():
        if not rows:
            continue
        table = SNOWFLAKE_TABLES[resource_type]
        flat_rows = [FLATTENERS[resource_type](r) for r in rows]
        df = pd.DataFrame(flat_rows)
        cols = ", ".join(df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))
        sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
        data = [tuple(row) for row in df.itertuples(index=False)]
        cur.executemany(sql, data)
        log.info(f"Loaded {len(data)} rows into {table}")
    conn.commit()
    cur.close()


def main():
    parser = argparse.ArgumentParser(description="Ingest FHIR data from Metriport API")
    parser.add_argument("--patient-ids", required=True, help="CSV file with patient_id column")
    parser.add_argument("--output", choices=["snowflake", "json"], default="json")
    parser.add_argument("--date-from", help="Filter resources from date (YYYY-MM-DD)")
    parser.add_argument("--date-to", help="Filter resources to date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Parse only, don't write")
    args = parser.parse_args()

    client = MetriportFHIRClient(
        api_key=os.environ["METRIPORT_API_KEY"],
        facility_id=os.environ["METRIPORT_FACILITY_ID"],
    )

    patient_ids = pd.read_csv(args.patient_ids)["patient_id"].tolist()
    log.info(f"Processing {len(patient_ids)} patients")

    all_resources: dict[str, list[dict]] = {rt: [] for rt in RESOURCE_TYPES}

    for pid in patient_ids:
        try:
            bundle = client.wait_for_consolidated(pid)
            parsed = parse_fhir_bundle(bundle)
            for rt, rows in parsed.items():
                all_resources[rt].extend(rows)
        except Exception as e:
            log.error(f"Failed to process patient {pid}: {e}")
            continue

    log.info(f"Total resources: { {rt: len(v) for rt, v in all_resources.items()} }")

    if args.dry_run:
        log.info("Dry run — skipping write")
        return

    if args.output == "snowflake":
        conn = snowflake.connector.connect(
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
            database=os.environ["SNOWFLAKE_DATABASE"],
        )
        load_to_snowflake(all_resources, conn)
        conn.close()
    else:
        with open("fhir_raw_output.json", "w") as f:
            json.dump(all_resources, f, indent=2, default=str)
        log.info("Wrote fhir_raw_output.json")


if __name__ == "__main__":
    main()
