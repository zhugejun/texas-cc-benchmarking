-- fct_student_outcomes.sql
-- Fact table combining all student outcome metrics for Texas community colleges

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

final as (
    select
        i.unitid,
        i.institution_name,

        -- Completion metrics (cast to larger integers)
        c.total_completions::number(10,0) as total_completions,
        c.associate_degrees::number(10,0) as associate_degrees,
        c.total_certificates::number(10,0) as total_certificates,
        c.certificates_under_1yr::number(10,0) as certificates_under_1yr,
        c.certificates_1_2yr::number(10,0) as certificates_1_2yr,

        -- Completion demographics
        c.pct_associate_hispanic,
        c.pct_associate_black,
        c.pct_associate_white,
        c.pct_associate_men,
        c.pct_associate_women,

        -- Graduation rates
        g.graduation_rate_150,
        g.transfer_out_rate,
        g.success_rate,

        -- Graduation rates by demographics
        g.grad_rate_hispanic,
        g.grad_rate_black,
        g.grad_rate_white,
        g.grad_rate_men,
        g.grad_rate_women,

        -- Equity gaps
        g.equity_gap_hispanic,
        g.equity_gap_black,

        -- Retention rates
        r.full_time_retention_rate,
        r.part_time_retention_rate,
        r.overall_retention_rate,

        -- Cohort sizes (for weighted averages - cast to larger integers)
        g.adjusted_cohort_total::number(10,0) as graduation_cohort_size,
        r.total_entering_undergrads::number(10,0) as retention_cohort_size

    from institutions i
    left join completions c on i.unitid = c.unitid
    left join graduation g on i.unitid = g.unitid
    left join retention r on i.unitid = r.unitid
)

select * from final
