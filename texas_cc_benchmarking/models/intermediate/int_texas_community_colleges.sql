-- int_texas_community_colleges.sql
-- Filter to Texas public 2-year institutions only

with institutions as (
    select * from {{ ref('stg_ipeds__institutions') }}
),

texas_ccs as (
    select
        unitid,
        institution_name,
        city,
        state_code,
        sector,
        sector_name,
        control_name,
        locale_type,
        institution_size_category,
        website_url,
        
        -- Size category description
        case institution_size_category
            when 1 then 'Under 1,000'
            when 2 then '1,000 - 4,999'
            when 3 then '5,000 - 9,999'
            when 4 then '10,000 - 19,999'
            when 5 then '20,000 and above'
            else 'Not reported'
        end as size_category_name

    from institutions
    where state_code = 'TX'
        and active_current_year = 1
        and (
                       -- Standard public 2-year colleges
            sector = 4
            
            -- OR public + 2-year institution level
            or (control = 1 and institution_level = 2)
            
            -- OR public institutions with Carnegie associate's college classification
            -- (catches CCs like Grayson that offer bachelor's but are primarily associate's)
            or (control = 1 and carnegie_basic_2021 between 2 and 10)
            
            -- OR public institutions where highest degree is primarily associate's
            or (control = 1 and highest_degree_offered = 3)  -- Associate degree
            
            -- OR institution name contains "community college" or "junior college"
            or (control = 1 
                and state_code = 'TX' 
                and (
                    lower(institution_name) like '%community college%'
                    or lower(institution_name) like '%junior college%'
                    or lower(institution_name) like '%technical college%'
                )
            )
        )
)

select * from texas_ccs
