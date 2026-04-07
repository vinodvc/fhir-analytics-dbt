-- mart_population_health.sql
-- -----------------------------------------------------------------------
-- Mart: Population Health KPIs
--
-- Aggregates patient-level clinical and utilization data into cohort-level
-- KPIs for the Metriport customer-facing analytics dashboard.
--
-- Grain: one row per (state, condition_category, urban_rural) cohort.
-- A "total" row per condition_category is also included (state = 'ALL').
--
-- This is the mart that would power a self-service dashboard where a
-- customer like Strive Health or Color can slice their patient population
-- and answer: "What % of my diabetic patients in rural states have an
-- open HbA1c care gap?"
-- -----------------------------------------------------------------------

with spine as (

    select * from {{ ref('int_patient_spine') }}

),

care_gaps as (

    select
        patient_id,
        condition_type,
        gap_flag,
        days_since_gap_obs,
        risk_tier,

    from {{ ref('mart_care_gaps') }}

),

utilization as (

    select
        patient_id,
        ed_visits_12mo,
        ip_admissions_12mo,
        preventable_ed_visits_12mo,
        is_ed_high_utilizer,
        utilization_tier,

    from {{ ref('mart_utilization') }}

),

-- -----------------------------------------------------------------------
-- Expand patient spine to one row per condition category
-- (so we can aggregate diabetes patients separately from hypertension)
-- -----------------------------------------------------------------------

patient_conditions as (

    select
        s.patient_id,
        s.state,
        s.urban_rural_classification,
        s.age_years,
        s.gender,
        s.n_chronic_conditions,
        s.n_active_medications,
        cond.condition_type,
        cond.gap_flag,
        cond.days_since_gap_obs,
        cond.risk_tier,
        u.ed_visits_12mo,
        u.ip_admissions_12mo,
        u.preventable_ed_visits_12mo,
        u.is_ed_high_utilizer,

    from spine s
    inner join care_gaps cond using (patient_id)   -- only patients with a qualifying condition
    left join utilization u using (patient_id)

),

-- -----------------------------------------------------------------------
-- Cohort aggregation with ROLLUP for total rows
-- -----------------------------------------------------------------------

cohort_agg as (

    select
        coalesce(state, 'ALL')                                      as state,
        condition_type,
        coalesce(urban_rural_classification, 'ALL')                 as urban_rural_classification,

        -- Patient counts
        count(distinct patient_id)                                  as patient_count,
        count(distinct case when age_years >= 65 then patient_id end)
                                                                    as patient_count_65_plus,

        -- Care gap KPIs
        count(distinct case when gap_flag then patient_id end)      as patients_with_open_gap,
        round(
            count(distinct case when gap_flag then patient_id end)::float
            / nullif(count(distinct patient_id), 0), 4
        )                                                           as gap_rate,

        count(distinct case when risk_tier = 'critical' then patient_id end)
                                                                    as critical_gap_count,
        count(distinct case when risk_tier = 'high' then patient_id end)
                                                                    as high_gap_count,

        avg(case when gap_flag then days_since_gap_obs end)         as avg_days_since_obs_gap_patients,

        -- Utilization KPIs
        sum(ed_visits_12mo)                                         as total_ed_visits,
        avg(ed_visits_12mo)                                         as avg_ed_visits_per_patient,
        sum(ip_admissions_12mo)                                     as total_ip_admissions,
        sum(preventable_ed_visits_12mo)                             as total_preventable_ed,
        round(
            sum(preventable_ed_visits_12mo)::float
            / nullif(sum(ed_visits_12mo), 0), 4
        )                                                           as preventable_ed_rate,
        count(distinct case when is_ed_high_utilizer then patient_id end)
                                                                    as high_utilizer_count,

        -- Clinical complexity
        avg(n_chronic_conditions)                                   as avg_chronic_conditions,
        avg(n_active_medications)                                   as avg_active_medications,

    from patient_conditions
    group by grouping sets (
        (state, condition_type, urban_rural_classification),        -- full breakdown
        (condition_type, urban_rural_classification),               -- across all states
        (state, condition_type),                                    -- across urban/rural
        (condition_type)                                            -- condition total only
    )

)

select
    {{ dbt_utils.generate_surrogate_key(['state', 'condition_type', 'urban_rural_classification']) }}
                                                                    as cohort_id,
    state,
    condition_type,
    urban_rural_classification,
    patient_count,
    patient_count_65_plus,
    patients_with_open_gap,
    gap_rate,
    critical_gap_count,
    high_gap_count,
    avg_days_since_obs_gap_patients,
    total_ed_visits,
    avg_ed_visits_per_patient,
    total_ip_admissions,
    total_preventable_ed,
    preventable_ed_rate,
    high_utilizer_count,
    avg_chronic_conditions,
    avg_active_medications,
    current_timestamp()                                             as _dbt_updated_at

from cohort_agg
