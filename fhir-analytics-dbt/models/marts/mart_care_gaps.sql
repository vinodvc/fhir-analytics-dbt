-- mart_care_gaps.sql
-- -----------------------------------------------------------------------
-- Mart: Care Gap Identification
--
-- A "care gap" exists when a patient with a qualifying condition has not
-- had the relevant measurement taken within the required timeframe.
-- Logic follows HEDIS-adjacent definitions.
--
-- Gap definitions:
--   diabetes       → HbA1c (LOINC 4548-4) not in last 365 days
--   hypertension   → BP panel (LOINC 55284-4) not in last 365 days
--   depression     → PHQ-9 (LOINC 44249-1) not in last 365 days
--
-- One row per patient per gap type.
-- Patients without qualifying conditions are excluded.
-- -----------------------------------------------------------------------

with spine as (

    select * from {{ ref('int_patient_spine') }}

),

observations as (

    select * from {{ ref('stg_fhir__observations') }}

),

-- -----------------------------------------------------------------------
-- Most recent relevant observation per patient per gap type
-- -----------------------------------------------------------------------

hba1c_obs as (
    select
        patient_id,
        max(effective_datetime)                               as last_hba1c_date,
        datediff('day', max(effective_datetime::date), current_date)
                                                              as days_since_hba1c,
    from observations
    where loinc_code in ('4548-4', '17856-6')           -- HbA1c variants
      and status = 'final'
    group by 1
),

bp_obs as (
    select
        patient_id,
        max(effective_datetime)                               as last_bp_date,
        datediff('day', max(effective_datetime::date), current_date)
                                                              as days_since_bp,
    from observations
    where loinc_code in ('55284-4', '8480-6', '8462-4') -- BP panel, systolic, diastolic
      and status = 'final'
    group by 1
),

phq9_obs as (
    select
        patient_id,
        max(effective_datetime)                               as last_phq9_date,
        datediff('day', max(effective_datetime::date), current_date)
                                                              as days_since_phq9,
    from observations
    where loinc_code in ('44249-1', '89204-2')           -- PHQ-9 variants
      and status = 'final'
    group by 1
),

-- -----------------------------------------------------------------------
-- Join spine to observation recency, apply gap logic
-- One row per patient per eligible gap type (UNION ALL pattern)
-- -----------------------------------------------------------------------

diabetes_gaps as (

    select
        s.patient_id,
        'diabetes'                                            as condition_type,
        'HbA1c not measured in past 12 months'                as gap_description,
        '4548-4'                                              as gap_loinc_code,
        s.age_years,
        s.gender,
        s.state,
        s.urban_rural_classification,
        s.n_chronic_conditions,
        s.n_ed_visits_12mo,
        s.n_ip_visits_12mo,
        s.n_active_medications,
        s.days_since_last_encounter,
        h.last_hba1c_date                                     as last_relevant_obs_date,
        coalesce(h.days_since_hba1c, 9999)                   as days_since_gap_obs,
        (coalesce(h.days_since_hba1c, 9999) >= 365)::boolean as gap_flag,

    from spine s
    left join hba1c_obs h using (patient_id)
    where s.has_diabetes_t2 or s.has_diabetes_t1

),

hypertension_gaps as (

    select
        s.patient_id,
        'hypertension'                                        as condition_type,
        'Blood pressure not recorded in past 12 months'       as gap_description,
        '55284-4'                                             as gap_loinc_code,
        s.age_years,
        s.gender,
        s.state,
        s.urban_rural_classification,
        s.n_chronic_conditions,
        s.n_ed_visits_12mo,
        s.n_ip_visits_12mo,
        s.n_active_medications,
        s.days_since_last_encounter,
        b.last_bp_date                                        as last_relevant_obs_date,
        coalesce(b.days_since_bp, 9999)                      as days_since_gap_obs,
        (coalesce(b.days_since_bp, 9999) >= 365)::boolean    as gap_flag,

    from spine s
    left join bp_obs b using (patient_id)
    where s.has_hypertension

),

depression_gaps as (

    select
        s.patient_id,
        'depression_screening'                                as condition_type,
        'PHQ-9 depression screening not completed in 12 months' as gap_description,
        '44249-1'                                             as gap_loinc_code,
        s.age_years,
        s.gender,
        s.state,
        s.urban_rural_classification,
        s.n_chronic_conditions,
        s.n_ed_visits_12mo,
        s.n_ip_visits_12mo,
        s.n_active_medications,
        s.days_since_last_encounter,
        p.last_phq9_date                                      as last_relevant_obs_date,
        coalesce(p.days_since_phq9, 9999)                    as days_since_gap_obs,
        (coalesce(p.days_since_phq9, 9999) >= 365)::boolean  as gap_flag,

    from spine s
    left join phq9_obs p using (patient_id)
    where s.has_mental_health_dx

),

unioned as (
    select * from diabetes_gaps
    union all
    select * from hypertension_gaps
    union all
    select * from depression_gaps
)

select
    {{ dbt_utils.generate_surrogate_key(['patient_id', 'condition_type']) }}
                                                              as care_gap_id,
    patient_id,
    condition_type,
    gap_description,
    gap_loinc_code,
    gap_flag,
    age_years,
    gender,
    state,
    urban_rural_classification,
    n_chronic_conditions,
    n_ed_visits_12mo,
    n_ip_visits_12mo,
    n_active_medications,
    days_since_last_encounter,
    last_relevant_obs_date,
    days_since_gap_obs,
    -- Risk tier for outreach prioritization (used before ML scores are available)
    case
        when days_since_gap_obs >= 730 then 'critical'
        when days_since_gap_obs >= 365 then 'high'
        when days_since_gap_obs >= 180 then 'medium'
        else 'low'
    end                                                       as risk_tier,
    current_timestamp()                                       as _dbt_updated_at

from unioned
