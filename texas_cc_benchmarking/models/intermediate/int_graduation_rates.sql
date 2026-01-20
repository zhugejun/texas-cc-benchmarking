-- int_graduation_rates.sql
-- Graduation rates for 2-year institutions (Texas community colleges)

with graduation_rates as (
    select * from {{ ref('stg_ipeds__graduation_rates') }}
),

texas_ccs as (
    select unitid from {{ ref('int_texas_community_colleges') }}
),

-- Get adjusted cohort (denominator) - cohort_type 29
adjusted_cohort as (
    select
        g.unitid,
        g.total as adjusted_cohort_total,
        g.total_men as cohort_men,
        g.total_women as cohort_women,
        g.hispanic_total as cohort_hispanic,
        g.black_total as cohort_black,
        g.white_total as cohort_white,
        g.asian_total as cohort_asian,
        g.amer_indian_total as cohort_amer_indian,
        g.two_or_more_total as cohort_two_or_more
    from graduation_rates g
    inner join texas_ccs t on g.unitid = t.unitid
    where g.cohort_type = 29  -- Adjusted cohort [2-YEAR]
),

-- Get completers within 150% time (numerator) - cohort_type 30
completers_150 as (
    select
        g.unitid,
        g.total as completers_150_total,
        g.total_men as completers_men,
        g.total_women as completers_women,
        g.hispanic_total as completers_hispanic,
        g.black_total as completers_black,
        g.white_total as completers_white,
        g.asian_total as completers_asian,
        g.amer_indian_total as completers_amer_indian,
        g.two_or_more_total as completers_two_or_more
    from graduation_rates g
    inner join texas_ccs t on g.unitid = t.unitid
    where g.cohort_type = 30  -- Completers within 150% of normal time [2-YEAR]
),

-- Get transfer-out students - cohort_type 33
transfers as (
    select
        g.unitid,
        g.total as transfer_out_total,
        g.hispanic_total as transfer_hispanic,
        g.black_total as transfer_black,
        g.white_total as transfer_white
    from graduation_rates g
    inner join texas_ccs t on g.unitid = t.unitid
    where g.cohort_type = 33  -- Transfer-out students [2-YEAR]
),

final as (
    select
        ac.unitid,

        -- Cohort sizes
        ac.adjusted_cohort_total,
        c.completers_150_total,
        tr.transfer_out_total,

        -- Overall graduation rate (150% time)
        round(c.completers_150_total * 100.0 / nullif(ac.adjusted_cohort_total, 0), 1) as graduation_rate_150,

        -- Transfer-out rate
        round(tr.transfer_out_total * 100.0 / nullif(ac.adjusted_cohort_total, 0), 1) as transfer_out_rate,

        -- Success rate (completers + transfers)
        round((c.completers_150_total + coalesce(tr.transfer_out_total, 0)) * 100.0
              / nullif(ac.adjusted_cohort_total, 0), 1) as success_rate,

        -- Graduation rates by race/ethnicity (HB8 equity metrics)
        round(c.completers_hispanic * 100.0 / nullif(ac.cohort_hispanic, 0), 1) as grad_rate_hispanic,
        round(c.completers_black * 100.0 / nullif(ac.cohort_black, 0), 1) as grad_rate_black,
        round(c.completers_white * 100.0 / nullif(ac.cohort_white, 0), 1) as grad_rate_white,
        round(c.completers_asian * 100.0 / nullif(ac.cohort_asian, 0), 1) as grad_rate_asian,

        -- Graduation rates by gender
        round(c.completers_men * 100.0 / nullif(ac.cohort_men, 0), 1) as grad_rate_men,
        round(c.completers_women * 100.0 / nullif(ac.cohort_women, 0), 1) as grad_rate_women,

        -- Equity gaps (difference from white graduation rate)
        round(c.completers_white * 100.0 / nullif(ac.cohort_white, 0), 1)
            - round(c.completers_hispanic * 100.0 / nullif(ac.cohort_hispanic, 0), 1) as equity_gap_hispanic,
        round(c.completers_white * 100.0 / nullif(ac.cohort_white, 0), 1)
            - round(c.completers_black * 100.0 / nullif(ac.cohort_black, 0), 1) as equity_gap_black,

        -- Raw demographic counts for cohort
        ac.cohort_hispanic,
        ac.cohort_black,
        ac.cohort_white,
        ac.cohort_men,
        ac.cohort_women

    from adjusted_cohort ac
    left join completers_150 c on ac.unitid = c.unitid
    left join transfers tr on ac.unitid = tr.unitid
)

select * from final
