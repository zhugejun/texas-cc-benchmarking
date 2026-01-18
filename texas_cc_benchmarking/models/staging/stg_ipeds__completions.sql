-- stg_ipeds__completions.sql
-- awards/degrees conferred by program, award level, and demographics

with source as (
    {{ dbt_utils.union_relations(
        relations=[
            source('raw_ipeds', 'c_a_2020'),
            source('raw_ipeds', 'c_a_2021'),
            source('raw_ipeds', 'c_a_2022'),
            source('raw_ipeds', 'c_a_2023'),
            source('raw_ipeds', 'c_a_2024'),
        ]
    ) }}
),

renamed as (
    select
        unitid,
        cipcode as cip_code,
        awlevel as award_level,
        year,

        case awlevel
            when 2 then 'Certificates of at least 1 but less than 2 years'
            when 3 then 'Associates degree'
            when 4 then 'Certificates of at least 2 but less than 4 years'
            when 5 then 'Bachelors degree'
            when 6 then 'Postbaccalaureate certificate'
            when 7 then 'Masters degree'
            when 8 then 'Post-masters certificate'
            when 17 then 'Doctors degree - research/scholarship'
            when 18 then 'Doctors degree - professional practice'
            when 19 then 'Doctors degree - other'
            when 20 then 'Certificates of less than 12 weeks'
            when 21 then 'Certificates of at least 12 weeks but less than 1 year'
            else 'Unknown'
        end as award_level_name,
        majornum as major_number,

        -- total completions
        ctotalt as total_completions,
        ctotalm as total_completions_men,
        ctotalw as total_completions_women,

        -- by race/ethnicity
        caiant as completions_american_indian_alaska_native,
        casiat as completions_asian,
        cbkaat as completions_black_african_american,
        chispt as completions_hispanic_latino,
        cnhpit as completions_native_hawaiian_pacific_islander,
        cwhitt as completions_white,
        c2mort as completions_two_or_more_races,
        cunknt as completions_unknown_race,
        cnralt as completions_non_resident_alien,

    from source
    where majornum = 1 -- first major only
        and year = 2024
)


select * from renamed