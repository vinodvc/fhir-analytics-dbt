-- stg_fhir__medications.sql
-- -----------------------------------------------------------------------
-- Staging: FHIR MedicationRequest resources
--
-- Source: raw_fhir.medication_requests
--
-- Key decisions:
--   * RxNorm code is the standard for medication identity across sources
--   * Metriport crosswalks medication codes to RxNorm where available
--   * medication_class derived from RxNorm code prefix patterns —
--     production would join to a full RxNorm classification seed
--   * is_high_risk_medication flag for polypharmacy analysis
--     (Beers Criteria–adjacent for patients 65+)
-- -----------------------------------------------------------------------

with source as (

    select * from {{ source('raw_fhir', 'medication_requests') }}

),

renamed as (

    select
        -- Keys
        medication_request_id,
        patient_id,

        -- Status / intent
        lower(status)                                               as status,
        lower(intent)                                               as intent,

        -- Medication identity
        nullif(trim(rxnorm_code), '')                               as rxnorm_code,
        nullif(trim(medication_display), '')                        as medication_display,

        -- Prescribing date
        try_cast(authored_on as date)                               as authored_on,
        try_cast(authored_on as date) >= dateadd('month', -12, current_date)
                                                                    as is_last_12mo,

        -- Dosage
        nullif(trim(dosage_text), '')                               as dosage_text,

        -- Derived: simplified medication class from display text
        -- Production: join to RxNorm drug class seed table
        case
            when lower(medication_display) like any ('%metformin%', '%insulin%', '%glipizide%',
                                                     '%glimepiride%', '%sitagliptin%')
                then 'antidiabetic'
            when lower(medication_display) like any ('%lisinopril%', '%amlodipine%', '%losartan%',
                                                     '%metoprolol%', '%atenolol%', '%hydrochlorothiazide%')
                then 'antihypertensive'
            when lower(medication_display) like any ('%atorvastatin%', '%rosuvastatin%', '%simvastatin%')
                then 'statin'
            when lower(medication_display) like any ('%sertraline%', '%fluoxetine%', '%escitalopram%',
                                                     '%bupropion%', '%venlafaxine%')
                then 'antidepressant'
            when lower(medication_display) like any ('%warfarin%', '%apixaban%', '%rivaroxaban%')
                then 'anticoagulant'
            when lower(medication_display) like any ('%oxycodone%', '%hydrocodone%', '%morphine%',
                                                     '%tramadol%', '%fentanyl%')
                then 'opioid'
            else 'other'
        end                                                         as medication_class,

        -- High-risk medication flag (Beers Criteria simplified)
        -- Useful for care gap / polypharmacy analysis in elderly patients
        (lower(medication_display) like any (
            '%benzodiazepine%', '%diazepam%', '%alprazolam%', '%lorazepam%',
            '%diphenhydramine%', '%zolpidem%', '%amitriptyline%'
        ))::boolean                                                 as is_beers_criteria_med,

        -- Metadata
        try_cast(_ingested_at as timestamp_ntz)                     as _ingested_at,

    from source

)

select *
from renamed
where
    -- Only include medication orders (not proposals or plans)
    intent in ('order', 'original-order', 'reflex-order', 'filler-order')
    and status in ('active', 'completed', 'stopped', 'on-hold')
