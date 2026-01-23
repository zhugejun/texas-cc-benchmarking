-- int_graduation_rates.sql
-- Graduation rates for Texas community colleges
-- Note: Some CCs report using 2-year cohort types (29, 30, 33)
-- while others report using 4-year cohort types (2, 3, 4)

with graduation_rates as (
    select * from {{ ref('stg_ipeds__graduation_rates') }}
),

texas_ccs as (
    select unitid from {{ ref('int_texas_community_colleges') }}
),

-- Get all relevant cohort types for Texas CCs
-- 2-year: 29 (adjusted), 30 (completers 150%), 33 (transfer)
-- 4-year style: 2 (adjusted), 3 (completers 150%), 4 (transfer)
texas_cc_graduation as (
    select
        g.unitid,
        g.year,  -- Include year for multi-year analysis
        g.cohort_type,
        g.total::number(10,0) as total,
        g.total_men::number(10,0) as total_men,
        g.total_women::number(10,0) as total_women,
        g.hispanic_total::number(10,0) as hispanic_total,
        g.black_total::number(10,0) as black_total,
        g.white_total::number(10,0) as white_total,
        g.asian_total::number(10,0) as asian_total,
        g.amer_indian_total::number(10,0) as amer_indian_total,
        g.two_or_more_total::number(10,0) as two_or_more_total
    from graduation_rates g
    inner join texas_ccs t on g.unitid = t.unitid
    where g.cohort_type in (2, 3, 4, 29, 30, 33)
),

-- Pivot to get one row per institution with all metrics
-- Prefer 2-year types (29, 30, 33) over 4-year types (2, 3, 4)
pivoted as (
    select
        unitid,
        year,  -- Include year for multi-year analysis
        -- Adjusted cohort (denominator)
        coalesce(
            max(case when cohort_type = 29 then total end),
            max(case when cohort_type = 2 then total end)
        ) as adjusted_cohort_total,
        coalesce(
            max(case when cohort_type = 29 then total_men end),
            max(case when cohort_type = 2 then total_men end)
        ) as cohort_men,
        coalesce(
            max(case when cohort_type = 29 then total_women end),
            max(case when cohort_type = 2 then total_women end)
        ) as cohort_women,
        coalesce(
            max(case when cohort_type = 29 then hispanic_total end),
            max(case when cohort_type = 2 then hispanic_total end)
        ) as cohort_hispanic,
        coalesce(
            max(case when cohort_type = 29 then black_total end),
            max(case when cohort_type = 2 then black_total end)
        ) as cohort_black,
        coalesce(
            max(case when cohort_type = 29 then white_total end),
            max(case when cohort_type = 2 then white_total end)
        ) as cohort_white,
        coalesce(
            max(case when cohort_type = 29 then asian_total end),
            max(case when cohort_type = 2 then asian_total end)
        ) as cohort_asian,

        -- Completers within 150% (numerator)
        coalesce(
            max(case when cohort_type = 30 then total end),
            max(case when cohort_type = 3 then total end)
        ) as completers_150_total,
        coalesce(
            max(case when cohort_type = 30 then total_men end),
            max(case when cohort_type = 3 then total_men end)
        ) as completers_men,
        coalesce(
            max(case when cohort_type = 30 then total_women end),
            max(case when cohort_type = 3 then total_women end)
        ) as completers_women,
        coalesce(
            max(case when cohort_type = 30 then hispanic_total end),
            max(case when cohort_type = 3 then hispanic_total end)
        ) as completers_hispanic,
        coalesce(
            max(case when cohort_type = 30 then black_total end),
            max(case when cohort_type = 3 then black_total end)
        ) as completers_black,
        coalesce(
            max(case when cohort_type = 30 then white_total end),
            max(case when cohort_type = 3 then white_total end)
        ) as completers_white,
        coalesce(
            max(case when cohort_type = 30 then asian_total end),
            max(case when cohort_type = 3 then asian_total end)
        ) as completers_asian,

        -- Transfer-out students
        coalesce(
            max(case when cohort_type = 33 then total end),
            max(case when cohort_type = 4 then total end)
        ) as transfer_out_total,
        coalesce(
            max(case when cohort_type = 33 then hispanic_total end),
            max(case when cohort_type = 4 then hispanic_total end)
        ) as transfer_hispanic,
        coalesce(
            max(case when cohort_type = 33 then black_total end),
            max(case when cohort_type = 4 then black_total end)
        ) as transfer_black,
        coalesce(
            max(case when cohort_type = 33 then white_total end),
            max(case when cohort_type = 4 then white_total end)
        ) as transfer_white

    from texas_cc_graduation
    group by unitid, year  -- Group by year for multi-year support
),

final as (
    select
        unitid,
        year,  -- Include year for multi-year analysis

        -- Cohort sizes
        adjusted_cohort_total,
        completers_150_total,
        transfer_out_total,

        -- Overall graduation rate (150% time)
        round(completers_150_total * 100.0 / nullif(adjusted_cohort_total, 0), 1) as graduation_rate_150,

        -- Transfer-out rate
        round(transfer_out_total * 100.0 / nullif(adjusted_cohort_total, 0), 1) as transfer_out_rate,

        -- Success rate (completers + transfers)
        round((completers_150_total + coalesce(transfer_out_total, 0)) * 100.0
              / nullif(adjusted_cohort_total, 0), 1) as success_rate,

        -- Graduation rates by race/ethnicity (HB8 equity metrics)
        round(completers_hispanic * 100.0 / nullif(cohort_hispanic, 0), 1) as grad_rate_hispanic,
        round(completers_black * 100.0 / nullif(cohort_black, 0), 1) as grad_rate_black,
        round(completers_white * 100.0 / nullif(cohort_white, 0), 1) as grad_rate_white,
        round(completers_asian * 100.0 / nullif(cohort_asian, 0), 1) as grad_rate_asian,

        -- Graduation rates by gender
        round(completers_men * 100.0 / nullif(cohort_men, 0), 1) as grad_rate_men,
        round(completers_women * 100.0 / nullif(cohort_women, 0), 1) as grad_rate_women,

        -- Equity gaps (difference from white graduation rate)
        round(completers_white * 100.0 / nullif(cohort_white, 0), 1)
            - round(completers_hispanic * 100.0 / nullif(cohort_hispanic, 0), 1) as equity_gap_hispanic,
        round(completers_white * 100.0 / nullif(cohort_white, 0), 1)
            - round(completers_black * 100.0 / nullif(cohort_black, 0), 1) as equity_gap_black,

        -- Raw demographic counts for cohort
        cohort_hispanic,
        cohort_black,
        cohort_white,
        cohort_men,
        cohort_women

    from pivoted
)

select * from final
