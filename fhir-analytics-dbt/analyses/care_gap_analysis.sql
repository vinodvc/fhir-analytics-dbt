-- analyses/care_gap_analysis.sql
-- -----------------------------------------------------------------------
-- Ad hoc analysis: Care Gap Closure Rate by Condition and Cohort
--
-- Business question: "Which patient cohorts have the worst care gap rates,
-- and is there a relationship between ED utilization and gap persistence?"
--
-- This is the type of analysis that would go into a customer QBR or
-- inform which patient segments to target for outreach first.
--
-- Run via: dbt compile --select care_gap_analysis
--          then execute the compiled SQL in Snowflake directly
-- -----------------------------------------------------------------------

with care_gaps as (

    select * from {{ ref('mart_care_gaps') }}

),

utilization as (

    select * from {{ ref('mart_utilization') }}

),

-- Join gaps to utilization to see if high ED utilizers are more likely
-- to have open gaps (hypothesis: they lack primary care access)
patient_level as (

    select
        g.patient_id,
        g.condition_type,
        g.gap_flag,
        g.days_since_gap_obs,
        g.risk_tier,
        g.age_years,
        g.gender,
        g.urban_rural_classification,
        g.n_chronic_conditions,
        u.ed_visits_12mo,
        u.preventable_ed_visits_12mo,
        u.utilization_tier,
        u.is_ed_high_utilizer,

    from care_gaps g
    left join utilization u using (patient_id)

),

-- -----------------------------------------------------------------------
-- Summary 1: Gap rate and avg days-since-obs by condition + urban/rural
-- -----------------------------------------------------------------------

gap_by_cohort as (

    select
        condition_type,
        urban_rural_classification,
        count(*)                                                as total_patients,
        sum(gap_flag::int)                                      as patients_with_gap,
        round(avg(gap_flag::float) * 100, 1)                    as gap_rate_pct,
        round(avg(case when gap_flag then days_since_gap_obs end), 0)
                                                                as avg_days_overdue,
        sum(case when risk_tier = 'critical' then 1 else 0 end) as critical_gaps,
        round(avg(ed_visits_12mo), 2)                           as avg_ed_visits,
        round(avg(case when gap_flag then ed_visits_12mo end), 2)
                                                                as avg_ed_visits_gap_patients,
        round(avg(case when not gap_flag then ed_visits_12mo end), 2)
                                                                as avg_ed_visits_no_gap,

    from patient_level
    group by 1, 2
    order by gap_rate_pct desc

),

-- -----------------------------------------------------------------------
-- Summary 2: Are ED high utilizers more likely to have open gaps?
-- (Stratified by utilization tier)
-- -----------------------------------------------------------------------

gap_by_utilization as (

    select
        condition_type,
        utilization_tier,
        count(*)                                                as patient_count,
        round(avg(gap_flag::float) * 100, 1)                    as gap_rate_pct,
        round(avg(days_since_gap_obs), 0)                       as avg_days_since_obs,

    from patient_level
    where utilization_tier is not null
    group by 1, 2
    order by condition_type, gap_rate_pct desc

),

-- -----------------------------------------------------------------------
-- Summary 3: Age band gap rates (are elderly patients worse off?)
-- -----------------------------------------------------------------------

gap_by_age_band as (

    select
        condition_type,
        case
            when age_years < 35         then '<35'
            when age_years between 35 and 49 then '35–49'
            when age_years between 50 and 64 then '50–64'
            when age_years between 65 and 74 then '65–74'
            else '75+'
        end                                                     as age_band,
        count(*)                                                as patient_count,
        round(avg(gap_flag::float) * 100, 1)                    as gap_rate_pct,
        round(avg(n_chronic_conditions), 1)                     as avg_comorbidities,

    from patient_level
    group by 1, 2
    order by 1, gap_rate_pct desc

)

-- -----------------------------------------------------------------------
-- Output: pivot all three summaries for dashboard ingestion
-- (In practice these would be three separate queries in a notebook or BI tool)
-- -----------------------------------------------------------------------

select 'by_cohort' as analysis_type, * from gap_by_cohort
union all
select 'by_utilization_tier', condition_type, utilization_tier,
       patient_count, gap_rate_pct, avg_days_since_obs,
       null, null, null, null, null
from gap_by_utilization
