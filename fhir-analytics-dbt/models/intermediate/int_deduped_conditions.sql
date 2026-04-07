-- int_deduped_conditions.sql
-- -----------------------------------------------------------------------
-- Intermediate: Deduplicated FHIR Condition records
--
-- Problem: Metriport aggregates records from multiple HIEs (CommonWell,
-- Carequality, direct EHR connections). The same clinical condition —
-- "Type 2 Diabetes, diagnosed Jan 2019" — may appear as 3–8 separate
-- FHIR Condition resources with different resource IDs, slightly
-- different onset dates (day vs month precision), and different
-- code systems (ICD-10 vs SNOMED).
--
-- Strategy:
--   1. Normalize to ICD-10 where possible (SNOMED→ICD-10 crosswalk
--      would live in a seed; simplified here to ICD-10 prefix matching)
--   2. Deduplicate by (patient_id, icd10_prefix_3, onset_year_month)
--      — same patient, same 3-digit ICD-10 chapter, same month of onset
--   3. For duplicates, keep the record with the most specific code
--      and earliest onset date
--   4. Count source_hie_count so marts can see how many HIEs confirmed it
--
-- This is materially the same logic Metriport applies in their own
-- consolidation layer — we're replicating it in dbt for warehouse use.
-- -----------------------------------------------------------------------

with conditions as (

    select * from {{ ref('stg_fhir__conditions') }}

),

ranked as (

    select
        *,
        -- Specificity score: longer ICD-10 code = more specific
        -- E11.65 is more specific than E11 — prefer longer
        len(icd10_code)                                         as code_specificity,

        -- Dedup key: patient + 3-digit ICD-10 chapter + onset month
        -- Allows E11.65 and E11.9 to be treated as the same condition
        concat_ws('|',
            patient_id,
            left(icd10_code, 3),
            to_char(onset_month, 'YYYY-MM')
        )                                                       as dedup_key,

        row_number() over (
            partition by
                patient_id,
                left(icd10_code, 3),       -- 3-digit ICD-10 chapter
                onset_month                 -- same month of onset
            order by
                len(icd10_code) desc,       -- prefer more specific codes
                onset_datetime asc,         -- prefer earliest onset
                recorded_date asc           -- tiebreak: earliest recorded
        )                                                       as dedup_rank,

        -- Count how many HIE sources reported this condition
        -- (used in downstream analytics to assess data confidence)
        count(*) over (
            partition by
                patient_id,
                left(icd10_code, 3),
                onset_month
        )                                                       as source_hie_count

    from conditions
    where icd10_code is not null            -- can't dedup without a standard code

),

deduped as (

    select
        condition_id,
        patient_id,
        clinical_status,
        verification_status,
        code_system,
        condition_code,
        condition_display,
        icd10_code,
        snomed_code,
        left(icd10_code, 3)                 as icd10_chapter,
        onset_datetime,
        onset_month,
        recorded_date,
        is_chronic_condition,
        condition_category,
        source_hie_count,
        (source_hie_count > 1)::boolean     as multi_hie_confirmed,
        dedup_key,
        _ingested_at,

    from ranked
    where dedup_rank = 1

)

select * from deduped
