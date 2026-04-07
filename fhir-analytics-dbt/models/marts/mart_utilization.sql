-- mart_utilization.sql
-- -----------------------------------------------------------------------
-- Mart: ED and Inpatient Utilization by Patient Cohort
--
-- Surfaces utilization patterns — ED visits, IP admissions, readmissions —
-- at both the patient and population level. Designed for:
--
--   1. Customer-facing analytics dashboard (self-service cohort analysis)
--   2. Anomaly detection input (high utilizers, unexpected cost spikes)
--   3. Care management targeting (flag high ED utilizers for outreach)
--
-- Grain: one row per patient, covering trailing 12 months of encounters.
--
-- "Potentially preventable ED" uses a simplified NYU ED classification
-- (flagged in stg_fhir__encounters). Full implementation would join to
-- the NYU ED Algorithm lookup table (available from SPARCS).
-- -----------------------------------------------------------------------

with spine as (

    select * from {{ ref('int_patient_spine') }}

),

encounters as (

    select * from {{ ref('stg_fhir__encounters') }}
    where is_last_12mo = true

),

-- -----------------------------------------------------------------------
-- Patient-level utilization aggregations (12-month window)
-- -----------------------------------------------------------------------

patient_utilization as (

    select
        patient_id,

        -- Volume
        count(distinct encounter_id)                                as total_encounters_12mo,
        count(distinct case when class_code = 'EMER' then encounter_id end)
                                                                    as ed_visits_12mo,
        count(distinct case when class_code = 'IMP'  then encounter_id end)
                                                                    as ip_admissions_12mo,
        count(distinct case when class_code = 'AMB'  then encounter_id end)
                                                                    as ambulatory_visits_12mo,
        count(distinct case when class_code = 'VR'   then encounter_id end)
                                                                    as telehealth_visits_12mo,

        -- Preventable ED
        count(distinct case
            when is_potentially_preventable_ed then encounter_id end)
                                                                    as preventable_ed_visits_12mo,

        -- After-hours / weekend (proxy for lack of primary care access)
        count(distinct case
            when class_code = 'AMB' and is_after_hours then encounter_id end)
                                                                    as after_hours_visits_12mo,
        count(distinct case
            when class_code = 'AMB' and is_weekend_encounter then encounter_id end)
                                                                    as weekend_visits_12mo,

        -- Length of stay (inpatient only)
        avg(case when class_code = 'IMP' then encounter_length_hours end)
                                                                    as avg_ip_los_hours,
        max(case when class_code = 'IMP' then encounter_length_hours end)
                                                                    as max_ip_los_hours,

        -- Readmission proxy: IP admission within 30 days of a prior IP discharge
        -- (Full readmission logic requires lag window across encounter pairs)
        sum(case
            when class_code = 'IMP'
             and lag(period_end, 1) over (
                 partition by patient_id, class_code
                 order by period_start
             ) >= dateadd('day', -30, period_start)
            then 1 else 0
        end)                                                        as ip_30d_readmissions,

        -- Most recent encounter dates by type
        max(case when class_code = 'EMER' then encounter_date end)  as last_ed_date,
        max(case when class_code = 'IMP'  then encounter_date end)  as last_ip_date,
        max(encounter_date)                                         as last_encounter_date_12mo,

    from encounters
    group by 1

),

-- -----------------------------------------------------------------------
-- Population-level percentiles (for anomaly detection)
-- -----------------------------------------------------------------------

population_stats as (

    select
        percentile_cont(0.50) within group (order by ed_visits_12mo)    as p50_ed,
        percentile_cont(0.75) within group (order by ed_visits_12mo)    as p75_ed,
        percentile_cont(0.90) within group (order by ed_visits_12mo)    as p90_ed,
        percentile_cont(0.95) within group (order by ed_visits_12mo)    as p95_ed,
        percentile_cont(0.50) within group (order by ip_admissions_12mo) as p50_ip,
        percentile_cont(0.90) within group (order by ip_admissions_12mo) as p90_ip,
        avg(ed_visits_12mo)                                             as avg_ed,
        stddev(ed_visits_12mo)                                          as stddev_ed,

    from patient_utilization

)

-- -----------------------------------------------------------------------
-- Final: join spine + utilization + anomaly flags
-- -----------------------------------------------------------------------

select
    -- Patient identity (from spine)
    s.patient_id,
    s.age_years,
    s.gender,
    s.state,
    s.urban_rural_classification,
    s.n_chronic_conditions,
    s.has_diabetes_t2,
    s.has_hypertension,
    s.n_active_medications,

    -- Utilization counts
    coalesce(u.total_encounters_12mo,       0) as total_encounters_12mo,
    coalesce(u.ed_visits_12mo,              0) as ed_visits_12mo,
    coalesce(u.ip_admissions_12mo,          0) as ip_admissions_12mo,
    coalesce(u.ambulatory_visits_12mo,      0) as ambulatory_visits_12mo,
    coalesce(u.telehealth_visits_12mo,      0) as telehealth_visits_12mo,
    coalesce(u.preventable_ed_visits_12mo,  0) as preventable_ed_visits_12mo,
    coalesce(u.after_hours_visits_12mo,     0) as after_hours_visits_12mo,
    coalesce(u.ip_30d_readmissions,         0) as ip_30d_readmissions,
    u.avg_ip_los_hours,
    u.last_ed_date,
    u.last_ip_date,

    -- Anomaly flags vs population percentiles
    (u.ed_visits_12mo >= p.p90_ed)::boolean    as is_ed_high_utilizer,
    (u.ed_visits_12mo >= p.p95_ed)::boolean    as is_ed_super_utilizer,
    (u.ip_admissions_12mo >= p.p90_ip)::boolean as is_ip_high_utilizer,

    -- Z-score for ED visits (for anomaly detection model)
    case
        when p.stddev_ed > 0
        then round((u.ed_visits_12mo - p.avg_ed) / p.stddev_ed, 3)
        else 0
    end                                         as ed_visits_zscore,

    -- Utilization tier (for outreach prioritization)
    case
        when u.ed_visits_12mo >= p.p95_ed      then 'super_utilizer'
        when u.ed_visits_12mo >= p.p90_ed      then 'high_utilizer'
        when u.ed_visits_12mo >= p.p75_ed      then 'moderate_utilizer'
        when u.ed_visits_12mo > 0              then 'low_utilizer'
        else 'no_ed_visits'
    end                                         as utilization_tier,

    -- Preventable ED rate
    case
        when u.ed_visits_12mo > 0
        then round(u.preventable_ed_visits_12mo::float / u.ed_visits_12mo, 3)
        else null
    end                                         as preventable_ed_rate,

    current_timestamp()                         as _dbt_updated_at

from spine s
left join patient_utilization u using (patient_id)
cross join population_stats p
