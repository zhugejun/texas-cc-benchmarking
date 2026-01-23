-- fct_student_outcomes.sql
-- Fact table combining all student outcome metrics for Texas community colleges
-- Now includes multi-year support

with institutions as (
    select unitid, institution_name from {{ ref('int_texas_community_colleges') }}
),

completions as (
    select * from {{ ref('int_completion_metrics') }}
),

graduation as (
    select * from {{ ref('int_graduation_rates') }}
),

retention as (
    select * from {{ ref('int_retention_rates') }}
),

-- Get all unique year/institution combinations from outcome data
outcome_years as (
    select distinct unitid, year from completions
    union
    select distinct unitid, year from graduation
    union
    select distinct unitid, year from retention
),

final as (
    select
        i.unitid,
        i.institution_name,
        oy.year,  -- Include year for multi-year analysis

        -- Completion metrics (cast to larger integers)
        c.total_completions::number(10,0) as total_completions,
        c.associate_degrees::number(10,0) as associate_degrees,
        c.total_certificates::number(10,0) as total_certificates,
        c.certificates_under_1yr::number(10,0) as certificates_under_1yr,
        c.certificates_1_2yr::number(10,0) as certificates_1_2yr,

        -- Completion demographics (percentages - use number(10,2))
        c.pct_associate_hispanic::number(10,2) as pct_associate_hispanic,
        c.pct_associate_black::number(10,2) as pct_associate_black,
        c.pct_associate_white::number(10,2) as pct_associate_white,
        c.pct_associate_men::number(10,2) as pct_associate_men,
        c.pct_associate_women::number(10,2) as pct_associate_women,

        -- Graduation rates (use number(10,2))
        g.graduation_rate_150::number(10,2) as graduation_rate_150,
        g.transfer_out_rate::number(10,2) as transfer_out_rate,
        g.success_rate::number(10,2) as success_rate,

        -- Graduation rates by demographics
        g.grad_rate_hispanic::number(10,2) as grad_rate_hispanic,
        g.grad_rate_black::number(10,2) as grad_rate_black,
        g.grad_rate_white::number(10,2) as grad_rate_white,
        g.grad_rate_men::number(10,2) as grad_rate_men,
        g.grad_rate_women::number(10,2) as grad_rate_women,

        -- Equity gaps
        g.equity_gap_hispanic::number(10,2) as equity_gap_hispanic,
        g.equity_gap_black::number(10,2) as equity_gap_black,

        -- Retention rates
        r.full_time_retention_rate::number(10,2) as full_time_retention_rate,
        r.part_time_retention_rate::number(10,2) as part_time_retention_rate,
        r.overall_retention_rate::number(10,2) as overall_retention_rate,

        -- Cohort sizes (for weighted averages - cast to larger integers)
        g.adjusted_cohort_total::number(10,0) as graduation_cohort_size,
        r.total_entering_undergrads::number(10,0) as retention_cohort_size

    from outcome_years oy
    inner join institutions i on oy.unitid = i.unitid
    left join completions c on oy.unitid = c.unitid and oy.year = c.year
    left join graduation g on oy.unitid = g.unitid and oy.year = g.year
    left join retention r on oy.unitid = r.unitid and oy.year = r.year
)

select * from final
