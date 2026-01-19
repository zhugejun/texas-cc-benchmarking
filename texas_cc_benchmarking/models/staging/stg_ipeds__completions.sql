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
        majornum as major_number,
        year,
        
        -- Award level descriptions (community colleges primarily award levels 1-3)
        case awlevel
            when 1 then 'Award < 1 year'
            when 2 then 'Award 1-2 years'
            when 3 then 'Associate degree'  -- PRIMARY HB8 METRIC for community colleges
            when 4 then 'Award 2-4 years'
            when 5 then 'Bachelors degree'
            when 6 then 'Postbaccalaureate certificate'
            when 7 then 'Masters degree'
            when 8 then 'Post-masters certificate'
            when 9 then 'Doctors degree - research'
            when 10 then 'Doctors degree - professional'
            when 11 then 'Doctors degree - other'
            when 12 then 'Award 4+ years'
            else 'Unknown'
        end as award_level_name,
        
        -- Total completions (all genders)
        ctotalt as total_completions,
        ctotalm as total_completions_men,
        ctotalw as total_completions_women,
        
        -- American Indian or Alaska Native (critical for HB8 equity metrics)
        caiant as amer_indian_total,
        caianm as amer_indian_men,
        caianw as amer_indian_women,
        
        -- Asian
        casiat as asian_total,
        casiam as asian_men,
        casiaw as asian_women,
        
        -- Black or African American (key HB8 equity population)
        cbkaat as black_total,
        cbkaam as black_men,
        cbkaaw as black_women,
        
        -- Hispanic or Latino (MOST IMPORTANT for Texas CC completions - largest group)
        chispt as hispanic_total,
        chispm as hispanic_men,
        chispw as hispanic_women,
        
        -- Native Hawaiian or Other Pacific Islander
        cnhpit as native_hawaiian_total,
        cnhpim as native_hawaiian_men,
        cnhpiw as native_hawaiian_women,
        
        -- White
        cwhitt as white_total,
        cwhitm as white_men,
        cwhitw as white_women,
        
        -- Two or more races
        c2mort as two_or_more_total,
        c2morm as two_or_more_men,
        c2morw as two_or_more_women,
        
        -- Race/ethnicity unknown
        cunknt as race_unknown_total,
        cunknm as race_unknown_men,
        cunknw as race_unknown_women,
        
        -- U.S. Nonresident (international students)
        cnralt as nonresident_total,
        cnralm as nonresident_men,
        cnralw as nonresident_women

    from source
    where majornum = 1 -- first major only
        and year = 2024
)


select * from renamed