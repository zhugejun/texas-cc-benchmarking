-- rpt_peer_comparison.sql
-- Peer comparison report with institution metrics and peer group averages

with institutions as (
    select * from {{ ref('dim_texas_institutions') }}
),

outcomes as (
    select * from {{ ref('fct_student_outcomes') }}
),

-- Calculate peer group averages
peer_averages as (
    select
        i.size_tier,
        i.is_hsi,
        i.pell_tier,
        i.urbanicity,

        -- Average metrics by peer group
        round(avg(o.graduation_rate_150), 1) as peer_avg_grad_rate,
        round(avg(o.success_rate), 1) as peer_avg_success_rate,
        round(avg(i.full_time_retention_rate), 1) as peer_avg_ft_retention,
        round(avg(i.overall_retention_rate), 1) as peer_avg_overall_retention,
        round(avg(o.associate_degrees), 0) as peer_avg_associate_degrees,
        round(avg(i.student_faculty_ratio), 1) as peer_avg_student_faculty_ratio,

        count(*) as peer_group_count

    from institutions i
    left join outcomes o on i.unitid = o.unitid
    group by i.size_tier, i.is_hsi, i.pell_tier, i.urbanicity
),

final as (
    select
        -- Institution info
        i.unitid,
        i.institution_name,
        i.city,

        -- Peer group classification
        i.size_tier,
        i.is_hsi,
        i.pell_tier,
        i.urbanicity,
        pa.peer_group_count,

        -- Institution metrics
        o.graduation_rate_150,
        o.success_rate,
        i.full_time_retention_rate,
        i.overall_retention_rate,
        o.associate_degrees,
        i.student_faculty_ratio,

        -- Peer group averages
        pa.peer_avg_grad_rate,
        pa.peer_avg_success_rate,
        pa.peer_avg_ft_retention,
        pa.peer_avg_overall_retention,
        pa.peer_avg_associate_degrees,
        pa.peer_avg_student_faculty_ratio,

        -- Difference from peer average
        round(o.graduation_rate_150 - pa.peer_avg_grad_rate, 1) as grad_rate_vs_peers,
        round(o.success_rate - pa.peer_avg_success_rate, 1) as success_rate_vs_peers,
        round(i.full_time_retention_rate - pa.peer_avg_ft_retention, 1) as ft_retention_vs_peers,
        round(i.overall_retention_rate - pa.peer_avg_overall_retention, 1) as overall_retention_vs_peers,

        -- Performance tier (above/at/below peer average)
        case
            when o.graduation_rate_150 >= pa.peer_avg_grad_rate + 5 then 'Above Peers'
            when o.graduation_rate_150 <= pa.peer_avg_grad_rate - 5 then 'Below Peers'
            else 'At Peer Average'
        end as grad_rate_performance

    from institutions i
    left join outcomes o on i.unitid = o.unitid
    left join peer_averages pa
        on i.size_tier = pa.size_tier
        and i.is_hsi = pa.is_hsi
        and i.pell_tier = pa.pell_tier
        and i.urbanicity = pa.urbanicity
)

select * from final
