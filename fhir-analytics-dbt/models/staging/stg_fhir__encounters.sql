-- stg_fhir__encounters.sql
-- -----------------------------------------------------------------------
-- Staging: FHIR Encounter resources
--
-- Source: raw_fhir.encounters
--
-- Key decisions:
--   * class_code is the most important field for utilization reporting:
--       IMP = inpatient, AMB = ambulatory, EMER = emergency, VR = virtual
--   * encounter_length_hours derived here for utilization mart
--   * Encounters without a period_start are excluded (tested in
--     tests/assert_no_null_encounter_dates.sql)
--   * is_preventable_ed is a proxy flag (NYU algorithm simplified):
--       ED visits for conditions treatable in primary care
-- -----------------------------------------------------------------------

with source as (

    select * from {{ source('raw_fhir', 'encounters') }}

),

renamed as (

    select
        -- Keys
        encounter_id,
        patient_id,

        -- Status
        lower(status)                                               as status,

        -- Encounter class (most important for utilization categorization)
        upper(nullif(trim(class_code), ''))                         as class_code,
        nullif(trim(class_display), '')                             as class_display,

        -- Encounter type
        nullif(trim(type_code), '')                                 as type_code,
        nullif(trim(type_display), '')                              as type_display,

        -- Temporal
        try_cast(period_start as timestamp_ntz)                     as period_start,
        try_cast(period_end as timestamp_ntz)                       as period_end,
        try_cast(period_start as date)                              as encounter_date,

        -- Derived: encounter duration in hours (null for ongoing / no end)
        datediff(
            'hour',
            try_cast(period_start as timestamp_ntz),
            try_cast(period_end as timestamp_ntz)
        )                                                           as encounter_length_hours,

        -- Derived: time-based flags for utilization analysis
        (
            dayofweek(try_cast(period_start as date)) in (0, 6)    -- Sunday=0, Saturday=6
        )::boolean                                                  as is_weekend_encounter,

        (
            hour(try_cast(period_start as timestamp_ntz)) between 18 and 23
            or hour(try_cast(period_start as timestamp_ntz)) between 0 and 6
        )::boolean                                                  as is_after_hours,

        -- Derived: surrogate flag for potentially preventable ED visit
        -- (simplified NYU ED algorithm — full version requires ICD-10 DRG crosswalk)
        (
            upper(class_code) = 'EMER'
            and lower(type_display) not like '%trauma%'
            and lower(type_display) not like '%cardiac arrest%'
        )::boolean                                                  as is_potentially_preventable_ed,

        -- Recent flag (trailing 12 months from today)
        (
            try_cast(period_start as date) >= dateadd('month', -12, current_date)
        )::boolean                                                  as is_last_12mo,

        -- Metadata
        try_cast(_ingested_at as timestamp_ntz)                     as _ingested_at,

    from source

)

select *
from renamed
where
    status = 'finished'
    and period_start is not null
