-- stg_fhir__patients.sql
-- -----------------------------------------------------------------------
-- Staging: FHIR Patient resources from the Metriport API ingestion layer
--
-- Source: raw_fhir.patients (loaded by scripts/ingest_metriport_fhir.py)
--
-- Key decisions:
--   * Cast all dates early — downstream marts assume proper date types
--   * Null-safe string coercion on name fields (FHIR name is optional)
--   * Derive age_years here so all marts use the same calculation
--   * urban_rural_classification derived from ZIP prefix (simplified;
--     production would join to HRSA RUCC codes via a seed table)
-- -----------------------------------------------------------------------

with source as (

    select * from {{ source('raw_fhir', 'patients') }}

),

renamed as (

    select
        -- Keys
        patient_id,

        -- Demographics
        nullif(trim(family_name), '')                              as last_name,
        nullif(trim(given_name), '')                               as first_name,
        try_cast(birth_date as date)                               as birth_date,
        datediff('year', try_cast(birth_date as date), current_date) as age_years,
        lower(gender)                                              as gender,

        -- Geography
        nullif(trim(postal_code), '')                              as postal_code,
        left(nullif(trim(postal_code), ''), 3)                     as zip3,
        nullif(trim(city), '')                                     as city,
        upper(nullif(trim(state), ''))                             as state,

        -- Derived: simplified urban/rural classification
        -- Production: join to seeds/hrsa_rucc_codes.csv
        case
            when left(postal_code, 1) in ('0','1','2','3','6','7','8','9') then 'urban'
            when left(postal_code, 1) in ('4','5')                         then 'suburban'
            else 'rural'
        end                                                        as urban_rural_classification,

        -- Record metadata
        active,
        try_cast(_ingested_at as timestamp_ntz)                    as _ingested_at,

    from source

),

deduped as (

    -- Metriport may return the same patient from multiple HIE sources.
    -- Keep the most recently ingested record per patient_id.
    select *,
        row_number() over (
            partition by patient_id
            order by _ingested_at desc
        ) as _row_num

    from renamed

)

select
    patient_id,
    last_name,
    first_name,
    birth_date,
    age_years,
    gender,
    postal_code,
    zip3,
    city,
    state,
    urban_rural_classification,
    active,
    _ingested_at

from deduped
where _row_num = 1
