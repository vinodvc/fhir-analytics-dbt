-- stg_fhir__conditions.sql
-- -----------------------------------------------------------------------
-- Staging: FHIR Condition resources (diagnoses) from Metriport API
--
-- Source: raw_fhir.conditions
--
-- Key decisions:
--   * Preserve both ICD-10-CM and SNOMED codes — Metriport surfaces both
--   * is_chronic flag derived from our ICD-10 chronic condition seed table
--   * onset_month used for deduplication in int_deduped_conditions
--   * Exclude unconfirmed / entered-in-error records at staging layer
-- -----------------------------------------------------------------------

with source as (

    select * from {{ source('raw_fhir', 'conditions') }}

),

chronic_codes as (

    -- Reference table: ICD-10 codes classified as chronic conditions
    -- Seeds: seeds/icd10_chronic_conditions.csv
    select icd10_code from {{ ref('icd10_chronic_conditions') }}

),

renamed as (

    select
        -- Keys
        condition_id,
        patient_id,

        -- Status filters
        lower(clinical_status)                              as clinical_status,
        lower(verification_status)                          as verification_status,

        -- Condition code
        upper(code_system)                                  as code_system,
        code                                                as condition_code,
        display                                             as condition_display,

        -- Derived: normalize to ICD-10 when possible
        case
            when upper(code_system) like '%ICD-10%'  then code
            when upper(code_system) like '%ICD10%'   then code
        end                                                 as icd10_code,

        case
            when upper(code_system) like '%SNOMED%'  then code
        end                                                 as snomed_code,

        -- Dates
        try_cast(onset_datetime as timestamp_ntz)           as onset_datetime,
        date_trunc('month', try_cast(onset_datetime as date)) as onset_month,
        try_cast(recorded_date as date)                     as recorded_date,

        -- Metadata
        try_cast(_ingested_at as timestamp_ntz)             as _ingested_at,

    from source

),

flagged as (

    select
        r.*,
        -- Flag chronic conditions using seed reference table
        (r.icd10_code in (select icd10_code from chronic_codes))::boolean as is_chronic_condition,

        -- Common condition categories used in care gap logic
        case
            when r.icd10_code like 'E11%'                   then 'diabetes_t2'
            when r.icd10_code like 'E10%'                   then 'diabetes_t1'
            when r.icd10_code like 'I10%'                   then 'hypertension'
            when r.icd10_code like 'F3%'                    then 'mental_health'
            when r.icd10_code like 'J4%'                    then 'asthma_copd'
            when r.icd10_code like 'N18%'                   then 'ckd'
            else 'other'
        end                                                 as condition_category,

    from renamed r

)

select *
from flagged
where
    -- Remove junk clinical statuses
    clinical_status in ('active', 'recurrence', 'relapse', 'inactive', 'remission', 'resolved')
    -- Remove unconfirmed / data entry errors
    and verification_status not in ('entered-in-error', 'refuted')
