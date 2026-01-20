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
        
        -- Total completions (all genders) - cast to handle large values
        ctotalt::number(10,0) as total_completions,
        ctotalm::number(10,0) as total_completions_men,
        ctotalw::number(10,0) as total_completions_women,

        -- American Indian or Alaska Native (critical for HB8 equity metrics)
        caiant::number(10,0) as amer_indian_total,
        caianm::number(10,0) as amer_indian_men,
        caianw::number(10,0) as amer_indian_women,

        -- Asian
        casiat::number(10,0) as asian_total,
        casiam::number(10,0) as asian_men,
        casiaw::number(10,0) as asian_women,

        -- Black or African American (key HB8 equity population)
        cbkaat::number(10,0) as black_total,
        cbkaam::number(10,0) as black_men,
        cbkaaw::number(10,0) as black_women,

        -- Hispanic or Latino (MOST IMPORTANT for Texas CC completions - largest group)
        chispt::number(10,0) as hispanic_total,
        chispm::number(10,0) as hispanic_men,
        chispw::number(10,0) as hispanic_women,

        -- Native Hawaiian or Other Pacific Islander
        cnhpit::number(10,0) as native_hawaiian_total,
        cnhpim::number(10,0) as native_hawaiian_men,
        cnhpiw::number(10,0) as native_hawaiian_women,

        -- White
        cwhitt::number(10,0) as white_total,
        cwhitm::number(10,0) as white_men,
        cwhitw::number(10,0) as white_women,

        -- Two or more races
        c2mort::number(10,0) as two_or_more_total,
        c2morm::number(10,0) as two_or_more_men,
        c2morw::number(10,0) as two_or_more_women,

        -- Race/ethnicity unknown
        cunknt::number(10,0) as race_unknown_total,
        cunknm::number(10,0) as race_unknown_men,
        cunknw::number(10,0) as race_unknown_women,

        -- U.S. Nonresident (international students)
        cnralt::number(10,0) as nonresident_total,
        cnralm::number(10,0) as nonresident_men,
        cnralw::number(10,0) as nonresident_women

    from source
    where majornum = 1 -- first major only
        and year = 2024
)


select * from renamed