-- int_peer_groups.sql
-- Create peer groupings based on size, locale, and student demographics

with texas_ccs as (
    select * from {{ ref('int_texas_community_colleges') }}
),

enrollment as (
    select
        unitid,
        total_enrollment::number(10,0) as total_enrollment,
        hispanic_total::number(10,0) as hispanic_total,
        black_total::number(10,0) as black_total,
        white_total::number(10,0) as white_total,

        -- Calculate demographic percentages
        round(hispanic_total * 100.0 / nullif(total_enrollment, 0), 1) as pct_hispanic,
        round(black_total * 100.0 / nullif(total_enrollment, 0), 1) as pct_black,
        round(white_total * 100.0 / nullif(total_enrollment, 0), 1) as pct_white

    from {{ ref('stg_ipeds__enrollment') }}
    where student_level = 1  -- All students total
        and year = (select max(year) from {{ ref('stg_ipeds__enrollment') }})  -- Most recent year
),

financial_aid as (
    select
        unitid,
        all_ug_pell_recipients_count::number(10,0) as all_ug_pell_recipients_count,
        undergrad_enrollment_aid::number(10,0) as undergrad_enrollment_aid,
        all_ug_pell_recipients_percent as pct_pell
    from {{ ref('stg_ipeds__financial_aid') }}
    where year = (select max(year) from {{ ref('stg_ipeds__financial_aid') }})  -- Most recent year
),

peer_groups as (
    select
        t.unitid,
        t.institution_name,
        t.city,
        t.locale_type,
        t.size_category_name,
        
        e.total_enrollment,
        e.pct_hispanic,
        e.pct_black,
        e.pct_white,
        
        f.pct_pell,
        
        -- Create peer group classifications
        
        -- Size tier
        case
            when e.total_enrollment < 5000 then 'Small'
            when e.total_enrollment < 15000 then 'Medium'
            else 'Large'
        end as size_tier,
        
        -- Hispanic-serving indicator (HSI = 25%+ Hispanic)
        case when e.pct_hispanic >= 25 then true else false end as is_hsi,
        
        -- High Pell indicator (above median ~40% for TX CCs)
        case when f.pct_pell >= 40 then 'High Pell' else 'Lower Pell' end as pell_tier,
        
        -- Urban vs Rural
        case 
            when t.locale_type in ('City', 'Suburb') then 'Urban/Suburban'
            else 'Town/Rural'
        end as urbanicity

    from texas_ccs t
    left join enrollment e on t.unitid = e.unitid
    left join financial_aid f on t.unitid = f.unitid
)

select * from peer_groups
