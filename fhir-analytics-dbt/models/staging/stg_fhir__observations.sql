-- stg_fhir__observations.sql
-- -----------------------------------------------------------------------
-- Staging: FHIR Observation resources (labs, vitals, screenings)
--
-- Source: raw_fhir.observations
--
-- Key decisions:
--   * LOINC code normalized from the coding array — Metriport crosswalks
--     to LOINC where available, so we prefer it over raw system codes
--   * Numeric values cast to float with unit preserved for downstream math
--   * Categorical values (e.g. "positive/negative") preserved separately
--   * Status filter excludes entered-in-error at staging layer
--   * effective_datetime is the clinically meaningful date — not ingested_at
-- -----------------------------------------------------------------------

with source as (

    select * from {{ source('raw_fhir', 'observations') }}

),

renamed as (

    select
        -- Keys
        observation_id,
        patient_id,

        -- Status
        lower(status)                                           as status,

        -- LOINC code (preferred) — populated by ingest script
        nullif(trim(loinc_code), '')                            as loinc_code,
        nullif(trim(display), '')                               as display,

        -- Clinical datetime (use for all temporal logic, not ingested_at)
        try_cast(effective_datetime as timestamp_ntz)           as effective_datetime,
        try_cast(effective_datetime as date)                    as effective_date,

        -- Numeric values (labs, vitals with units)
        try_cast(value_quantity as float)                       as value_quantity,
        nullif(trim(value_unit), '')                            as value_unit,

        -- Coded values (e.g., positive/negative, present/absent)
        nullif(trim(value_code), '')                            as value_code,
        nullif(trim(value_display), '')                         as value_display,

        -- Result interpretation (H=high, L=low, N=normal, A=abnormal)
        upper(nullif(trim(interpretation), ''))                 as interpretation_code,

        -- Derived: observation category for care gap matching
        case
            when loinc_code in ('4548-4', '17856-6')            then 'hba1c'
            when loinc_code in ('55284-4', '8480-6', '8462-4') then 'blood_pressure'
            when loinc_code in ('44249-1', '89204-2')           then 'depression_screening'
            when loinc_code like '718-%'                        then 'sdoh_screening'
            when value_unit in ('mg/dL', 'mmol/L', 'g/dL')     then 'lab_result'
            when value_unit in ('mm[Hg]', 'bpm', 'kg', 'cm')   then 'vital_sign'
            else 'other'
        end                                                     as observation_category,

        -- Metadata
        try_cast(_ingested_at as timestamp_ntz)                 as _ingested_at,

    from source

)

select *
from renamed
where
    -- Exclude data entry errors
    status not in ('entered-in-error', 'cancelled')
    -- Must have a clinical date for any temporal logic to work
    and effective_datetime is not null
