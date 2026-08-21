-- int_texas_community_colleges.sql
-- Filter to Texas public 2-year institutions only

with institutions as (
    select * from {{ ref('stg_ipeds__institutions') }}
),

-- Which UNITIDs count as Texas community colleges.
--
-- Evaluated across every loaded directory year, not just the newest one, because
-- a single year's rename or reclassification should not drop a college out of a
-- multi-year benchmark. Houston Community College is the case in point: IPEDS
-- has always filed it under sector 1 (public 4-year, since it awards bachelor's
-- degrees), so only the name rule below ever caught it -- and the 2025 directory
-- renamed it "Houston City College", which no longer matches.
qualifying as (
    select distinct unitid
    from institutions
    where state_code = 'TX'
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
                and (
                    lower(institution_name) like '%community college%'
                    or lower(institution_name) like '%junior college%'
                    or lower(institution_name) like '%technical college%'
                )
            )
        )
),

-- Describe each college with its most recent directory record, so names,
-- addresses, and size categories stay current.
latest as (
    select *
    from institutions
    where year = (select max(year) from institutions)
        and active_current_year = 1
        and state_code = 'TX'
),

texas_ccs as (
    select
        l.unitid,
        l.institution_name,
        l.city,
        l.state_code,
        l.latitude,
        l.longitude,
        l.sector,
        l.sector_name,
        l.control_name,
        l.locale_type,
        l.institution_size_category,
        l.website_url,

        -- Size category description
        case l.institution_size_category
            when 1 then 'Under 1,000'
            when 2 then '1,000 - 4,999'
            when 3 then '5,000 - 9,999'
            when 4 then '10,000 - 19,999'
            when 5 then '20,000 and above'
            else 'Not reported'
        end as size_category_name

    from latest l
    inner join qualifying q on q.unitid = l.unitid
)

select * from texas_ccs
