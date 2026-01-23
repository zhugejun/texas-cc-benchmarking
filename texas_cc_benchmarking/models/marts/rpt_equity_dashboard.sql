-- rpt_equity_dashboard.sql
-- Equity metrics dashboard for HB8 reporting

with institutions as (
    select * from {{ ref('dim_texas_institutions') }}
),

outcomes as (
    select * from {{ ref('fct_student_outcomes') }}
),

completions as (
    select * from {{ ref('int_completion_metrics') }}
),

final as (
    select
        -- Institution info
        i.unitid,
        i.institution_name,
        i.city,
        i.is_hsi,
        i.pell_tier,

        -- Enrollment demographics
        i.total_enrollment::number(10,0) as total_enrollment,
        i.pct_hispanic,
        i.pct_black,
        i.pct_white,
        i.pct_pell,

        -- Graduation rates by race/ethnicity
        o.graduation_rate_150 as overall_grad_rate,
        o.grad_rate_hispanic,
        o.grad_rate_black,
        o.grad_rate_white,

        -- Equity gaps (positive = gap exists, white outperforms)
        o.equity_gap_hispanic,
        o.equity_gap_black,

        -- Gap severity classification
        case
            when o.equity_gap_hispanic >= 10 then 'Large Gap (10%+)'
            when o.equity_gap_hispanic >= 5 then 'Moderate Gap (5-10%)'
            when o.equity_gap_hispanic > 0 then 'Small Gap (<5%)'
            when o.equity_gap_hispanic <= 0 then 'No Gap / Outperforming'
            else 'Insufficient Data'
        end as hispanic_gap_severity,

        case
            when o.equity_gap_black >= 10 then 'Large Gap (10%+)'
            when o.equity_gap_black >= 5 then 'Moderate Gap (5-10%)'
            when o.equity_gap_black > 0 then 'Small Gap (<5%)'
            when o.equity_gap_black <= 0 then 'No Gap / Outperforming'
            else 'Insufficient Data'
        end as black_gap_severity,

        -- Completion equity (associate degree demographics vs enrollment demographics)
        c.pct_associate_hispanic,
        c.pct_associate_black,
        c.pct_associate_white,

        -- Completion equity index (completion % / enrollment %)
        -- Values > 1 mean group is overrepresented in completions
        round(c.pct_associate_hispanic / nullif(i.pct_hispanic, 0), 2) as hispanic_completion_equity_index,
        round(c.pct_associate_black / nullif(i.pct_black, 0), 2) as black_completion_equity_index,
        round(c.pct_associate_white / nullif(i.pct_white, 0), 2) as white_completion_equity_index,

        -- Gender metrics
        o.grad_rate_men,
        o.grad_rate_women,
        o.pct_associate_men,
        o.pct_associate_women,

        -- Retention
        i.overall_retention_rate,

        -- Raw counts for weighted calculations
        o.graduation_cohort_size,
        c.associate_hispanic,
        c.associate_black,
        c.associate_white

    from institutions i
    left join outcomes o on i.unitid = o.unitid
    left join completions c on i.unitid = c.unitid
)

select * from final
