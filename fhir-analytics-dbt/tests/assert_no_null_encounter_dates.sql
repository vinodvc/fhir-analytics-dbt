-- tests/assert_no_null_encounter_dates.sql
-- Custom dbt test: all finished encounters must have a period_start date.
-- A null period_start breaks the patient spine's days_since_last_encounter
-- calculation and will silently corrupt care gap logic.

select
    encounter_id,
    patient_id,
    status,
    period_start

from {{ ref('stg_fhir__encounters') }}

where status = 'finished'
  and period_start is null
