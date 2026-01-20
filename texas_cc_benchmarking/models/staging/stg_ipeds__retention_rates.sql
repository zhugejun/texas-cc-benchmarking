-- stg_ipeds__retention_rates.sql
-- Retention rates and student-to-faculty ratio from IPEDS EF_D survey

with source as (
    {{ dbt_utils.union_relations(
        relations=[
            source('raw_ipeds', 'ef_d_2020'),
            source('raw_ipeds', 'ef_d_2021'),
            source('raw_ipeds', 'ef_d_2022'),
            source('raw_ipeds', 'ef_d_2023'),
            source('raw_ipeds', 'ef_d_2024'),
        ]
    ) }}
),

renamed as (
    select
        unitid,
        year,

        -- Retention rates (PRIMARY METRICS)
        ret_pcf as full_time_retention_rate,
        ret_pcp as part_time_retention_rate,

        -- Student-to-faculty ratio
        stufacr as student_faculty_ratio,

        -- Entering class context (cast to handle large enrollments)
        ugentern::number(10,0) as total_entering_undergrads,

        -- Full-time cohort details
        rrftcta::number(10,0) as ft_adjusted_cohort,
        ret_nmf::number(10,0) as ft_students_retained,

        -- Part-time cohort details
        rrptcta::number(10,0) as pt_adjusted_cohort,
        ret_nmp::number(10,0) as pt_students_retained,

        -- GRS cohort info (for linking to graduation rates)
        grcohrt::number(10,0) as grs_cohort_count,
        pgrcohrt as grs_cohort_pct_of_entering

    from source
    where year = 2024
)

select * from renamed