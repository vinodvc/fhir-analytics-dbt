"""
posthog_schema.py
-----------------
Defines the PostHog event schema for Metriport's productized analytics layer.

This schema mirrors the event taxonomy a Data Analyst would design when
instrumenting the Metriport platform — giving customers self-service visibility
into how their patient data is being accessed and where care gaps exist.

Usage:
    from scripts.posthog_schema import track_event, EVENTS
    track_event("patient_record_retrieved", {...})
"""

import os
import logging
from datetime import datetime
from typing import Any

import posthog

log = logging.getLogger(__name__)

posthog.project_api_key = os.getenv("POSTHOG_API_KEY", "")
posthog.host = os.getenv("POSTHOG_HOST", "https://app.posthog.com")

# ---------------------------------------------------------------------------
# Event taxonomy
# ---------------------------------------------------------------------------
# Each event maps to a dict of REQUIRED properties.
# Optional properties can always be added — this is the minimum schema.

EVENTS: dict[str, list[str]] = {

    # Fired when the Metriport API returns a consolidated FHIR bundle
    "patient_record_retrieved": [
        "patient_id",           # Metriport patient ID (hashed in PostHog)
        "customer_id",          # CX ID — Metriport's customer identifier
        "source_hie_count",     # How many HIEs responded (CommonWell, Carequality...)
        "resource_types",       # List of FHIR resource types returned
        "total_resource_count", # Total FHIR resources in bundle
        "latency_ms",           # End-to-end API latency
        "has_new_records",      # Whether new records were found vs cache
    ],

    # Fired when the care gap model flags a patient
    "care_gap_flagged": [
        "patient_id",
        "customer_id",
        "condition_type",       # diabetes | hypertension | depression_screening
        "days_since_obs",       # Days since the relevant observation
        "gap_type",             # loinc code of the missing measurement
        "risk_tier",            # critical | high | medium | low
        "ml_risk_score",        # Predicted probability from care_gap_model.py
    ],

    # Fired for every FHIR query executed via the Metriport API
    "fhir_query_executed": [
        "customer_id",
        "query_type",           # consolidated | single_resource | search
        "resource_type",        # Patient | Condition | Observation | ...
        "record_count",
        "duration_ms",
        "cache_hit",            # Whether result came from cache
    ],

    # Fired after Metriport consolidates and deduplicates a patient's FHIR bundle
    "document_consolidated": [
        "patient_id",
        "customer_id",
        "source_count",         # Raw FHIR resources before dedup
        "dedup_removed",        # Resources removed as duplicates
        "final_count",          # Final resource count after dedup
        "dedup_rate",           # dedup_removed / source_count
    ],

    # Fired when a customer views a population health dashboard
    "dashboard_viewed": [
        "customer_id",
        "dashboard_type",       # population_health | care_gaps | utilization
        "patient_count",        # Size of cohort displayed
        "filter_applied",       # Whether any filters were used
    ],

    # Fired when a customer exports data to their warehouse
    "warehouse_export_triggered": [
        "customer_id",
        "export_type",          # full | incremental | care_gaps_only
        "record_count",
        "destination",          # snowflake | bigquery | redshift
        "duration_ms",
    ],
}


def validate_event(event_name: str, properties: dict[str, Any]) -> list[str]:
    """
    Check that all required properties are present for an event.
    Returns list of missing property names.
    """
    if event_name not in EVENTS:
        raise ValueError(f"Unknown event: {event_name}. Valid events: {list(EVENTS.keys())}")
    required = EVENTS[event_name]
    return [p for p in required if p not in properties]


def track_event(
    event_name: str,
    properties: dict[str, Any],
    distinct_id: str = None,
    raise_on_missing: bool = False,
) -> None:
    """
    Track an event in PostHog.

    Args:
        event_name: Must be a key in EVENTS.
        properties: Event properties dict.
        distinct_id: PostHog distinct_id (defaults to customer_id from properties).
        raise_on_missing: If True, raise on missing required properties.
    """
    missing = validate_event(event_name, properties)
    if missing:
        msg = f"Event '{event_name}' missing required properties: {missing}"
        if raise_on_missing:
            raise ValueError(msg)
        log.warning(msg)

    # Always add these metadata properties
    properties["_schema_version"] = "1.0"
    properties["_tracked_at"] = datetime.utcnow().isoformat()

    did = distinct_id or properties.get("customer_id", "unknown")

    posthog.capture(
        distinct_id=did,
        event=event_name,
        properties=properties,
    )
    log.debug(f"PostHog event tracked: {event_name} (distinct_id={did})")


# ---------------------------------------------------------------------------
# Example usage / smoke test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Simulate the events fired during a single patient record retrieval flow
    track_event("patient_record_retrieved", {
        "patient_id":           "pt_abc123_hashed",
        "customer_id":          "cx_metriport_demo",
        "source_hie_count":     3,
        "resource_types":       ["Patient", "Condition", "Observation", "Encounter"],
        "total_resource_count": 247,
        "latency_ms":           1843,
        "has_new_records":      True,
    })

    track_event("document_consolidated", {
        "patient_id":   "pt_abc123_hashed",
        "customer_id":  "cx_metriport_demo",
        "source_count": 247,
        "dedup_removed": 38,
        "final_count":  209,
        "dedup_rate":   round(38/247, 3),
    })

    track_event("care_gap_flagged", {
        "patient_id":    "pt_abc123_hashed",
        "customer_id":   "cx_metriport_demo",
        "condition_type":"diabetes",
        "days_since_obs": 447,
        "gap_type":      "4548-4",
        "risk_tier":     "high",
        "ml_risk_score": 0.73,
    })

    log.info("PostHog schema smoke test complete.")
