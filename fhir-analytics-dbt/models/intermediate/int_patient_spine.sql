-- int_patient_spine.sql
-- -----------------------------------------------------------------------
-- Intermediate: One row per patient with all stable attributes pre-joined.
--
-- This is the foundation every mart joins to. Building the spine here
-- means mart queries stay clean and fast — no repeated joins to patients,
-- no fan-out risk from joining multiple fact tables at the mart layer.
--
-- Columns:
--   * Patient demographics (from stg_fhir__patients)
--   * Condition summary stats (from stg_fhir__conditions)
--   * Encounter summary stats (from stg_fhir__encounters)
--   * Medication summary stats (from stg_fhir__medications)
-- -----------------------------------------------------------------------

with patients as (

    select * from {{ ref('stg_fhir__patients') }}

),

conditions_agg as (

    select
        patient_id,
        count(distinct condition_id)                                as n_conditions_total,
        count(distinct case when is_chronic_condition then condition_id end)
                                                                    as n_chronic_conditions,
        count(distinct case when clinical_status = 'active' then condition_id end)
                                                                    as n_active_conditions,
        -- Condition category flags — used as features in care gap model
        max(case when condition_category = 'diabetes_t2'    then 1 else 0 end)::boolean
                                                                    as has_diabetes_t2,
        max(case when condition_category = 'diabetes_t1'    then 1 else 0 end)::boolean
                                                                    as has_diabetes_t1,
        max(case when condition_category = 'hypertension'   then 1 else 0 end)::boolean
                                                                    as has_hypertension,
        max(case when condition_category = 'mental_health'  then 1 else 0 end)::boolean
                                                                    as has_mental_health_dx,
        max(case when condition_category = 'ckd'            then 1 else 0 end)::boolean
                                                                    as has_ckd,

    from {{ ref('stg_fhir__conditions') }}
    group by 1

),

encounters_agg as (

    select
        patient_id,
        count(distinct encounter_id)                                as n_encounters_total,

        -- Last 12 months
        count(distinct case
            when period_start >= dateadd('month', -12, current_date)
            then encounter_id end)                                  as n_encounters_12mo,

        -- Encounter class breakdown (last 12 months)
        count(distinct case
            when class_code = 'EMER'
             and period_start >= dateadd('month', -12, current_date)
            then encounter_id end)                                  as n_ed_visits_12mo,

        count(distinct case
            when class_code = 'IMP'
             and period_start >= dateadd('month', -12, current_date)
            then encounter_id end)                                  as n_ip_visits_12mo,

        count(distinct case
            when class_code = 'AMB'
             and period_start >= dateadd('month', -12, current_date)
            then encounter_id end)                                  as n_ambulatory_visits_12mo,

        -- Recency
        max(period_start)                                           as last_encounter_date,
        datediff('day', max(period_start), current_date)            as days_since_last_encounter,

        -- Distinct providers (proxy for care fragmentation)
        count(distinct type_code)                                   as n_unique_encounter_types,

    from {{ ref('stg_fhir__encounters') }}
    where status = 'finished'
    group by 1

),

medications_agg as (

    select
        patient_id,
        count(distinct medication_request_id)                       as n_medications_total,
        count(distinct case
            when status = 'active' then medication_request_id end)  as n_active_medications,

    from {{ ref('stg_fhir__medications') }}
    group by 1

)

select
    -- Patient identity
    p.patient_id,
    p.last_name,
    p.first_name,
    p.birth_date,
    p.age_years,
    p.gender,
    p.postal_code,
    p.zip3,
    p.state,
    p.urban_rural_classification,

    -- Condition summary
    coalesce(c.n_conditions_total,    0)    as n_conditions_total,
    coalesce(c.n_chronic_conditions,  0)    as n_chronic_conditions,
    coalesce(c.n_active_conditions,   0)    as n_active_conditions,
    coalesce(c.has_diabetes_t2,  false)     as has_diabetes_t2,
    coalesce(c.has_diabetes_t1,  false)     as has_diabetes_t1,
    coalesce(c.has_hypertension, false)     as has_hypertension,
    coalesce(c.has_mental_health_dx, false) as has_mental_health_dx,
    coalesce(c.has_ckd, false)              as has_ckd,

    -- Encounter summary
    coalesce(e.n_encounters_total,         0)   as n_encounters_total,
    coalesce(e.n_encounters_12mo,          0)   as n_encounters_12mo,
    coalesce(e.n_ed_visits_12mo,           0)   as n_ed_visits_12mo,
    coalesce(e.n_ip_visits_12mo,           0)   as n_ip_visits_12mo,
    coalesce(e.n_ambulatory_visits_12mo,   0)   as n_ambulatory_visits_12mo,
    e.last_encounter_date,
    coalesce(e.days_since_last_encounter, 9999) as days_since_last_encounter,

    -- Medication summary
    coalesce(m.n_medications_total,  0)  as n_medications_total,
    coalesce(m.n_active_medications, 0)  as n_active_medications,

from patients p
left join conditions_agg  c using (patient_id)
left join encounters_agg  e using (patient_id)
left join medications_agg m using (patient_id)
