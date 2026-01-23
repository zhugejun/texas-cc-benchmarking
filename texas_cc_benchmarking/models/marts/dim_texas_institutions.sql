-- dim_texas_institutions.sql
-- Dimension table for Texas community colleges with all attributes and peer groupings

with institutions as (
    select * from {{ ref('int_texas_community_colleges') }}
),

peer_groups as (
    select * from {{ ref('int_peer_groups') }}
),

retention as (
    select * from {{ ref('int_retention_rates') }}
),

final as (
    select
        -- Institution identifiers
        i.unitid,
        i.institution_name,
        i.city,
        i.state_code,
        i.latitude,
        i.longitude,
        i.website_url,

        -- Institution classification
        i.sector_name,
        i.control_name,
        i.size_category_name,
        i.locale_type,

        -- Peer group attributes
        p.total_enrollment::number(10,0) as total_enrollment,
        p.size_tier,
        p.is_hsi,
        p.pell_tier,
        p.urbanicity,

        -- Demographics (cast percentages)
        p.pct_hispanic::number(10,2) as pct_hispanic,
        p.pct_black::number(10,2) as pct_black,
        p.pct_white::number(10,2) as pct_white,
        p.pct_pell::number(10,2) as pct_pell,

        -- Retention & efficiency metrics (cast rates)
        r.full_time_retention_rate::number(10,2) as full_time_retention_rate,
        r.part_time_retention_rate::number(10,2) as part_time_retention_rate,
        r.overall_retention_rate::number(10,2) as overall_retention_rate,
        r.student_faculty_ratio::number(10,2) as student_faculty_ratio

    from institutions i
    left join peer_groups p on i.unitid = p.unitid
    left join retention r on i.unitid = r.unitid
)

select * from final
