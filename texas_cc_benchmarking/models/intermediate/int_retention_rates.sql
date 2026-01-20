-- int_retention_rates.sql
-- Retention rates for Texas community colleges

with retention as (
    select * from {{ ref('stg_ipeds__retention_rates') }}
),

texas_ccs as (
    select unitid from {{ ref('int_texas_community_colleges') }}
),

final as (
    select
        r.unitid,

        -- Primary retention metrics
        r.full_time_retention_rate,
        r.part_time_retention_rate,

        -- Blended retention rate (weighted by cohort size)
        round(
            (coalesce(r.ft_students_retained, 0) + coalesce(r.pt_students_retained, 0)) * 100.0
            / nullif(coalesce(r.ft_adjusted_cohort, 0) + coalesce(r.pt_adjusted_cohort, 0), 0),
            1
        ) as overall_retention_rate,

        -- Student-to-faculty ratio
        r.student_faculty_ratio,

        -- Cohort context (cast to larger integers)
        r.total_entering_undergrads::number(10,0) as total_entering_undergrads,
        r.ft_adjusted_cohort::number(10,0) as ft_adjusted_cohort,
        r.pt_adjusted_cohort::number(10,0) as pt_adjusted_cohort,
        (r.ft_adjusted_cohort + r.pt_adjusted_cohort)::number(10,0) as total_adjusted_cohort,

        -- Students retained counts
        r.ft_students_retained::number(10,0) as ft_students_retained,
        r.pt_students_retained::number(10,0) as pt_students_retained,

        -- GRS cohort linkage (for joining with graduation rates)
        r.grs_cohort_count::number(10,0) as grs_cohort_count,
        r.grs_cohort_pct_of_entering

    from retention r
    inner join texas_ccs t on r.unitid = t.unitid
)

select * from final
