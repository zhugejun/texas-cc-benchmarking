-- stg_ipeds__enrollment.sql
-- enrollment by program, award level, and demographics

with source as (
    {{ dbt_utils.union_relations(
        relations=[
            source('raw_ipeds', 'effy_2020'),
            source('raw_ipeds', 'effy_2021'),
            source('raw_ipeds', 'effy_2022'),
            source('raw_ipeds', 'effy_2023'),
            source('raw_ipeds', 'effy_2024'),
        ]
    ) }}
),


renamed as (
    select
        unitid,
        effyalev as student_level,
        effylev as undergrad_or_grad_level,
        lstudy as original_level_of_study,
        year,
        
        case effyalev
            when 1 then 'All students'
            when 2 then 'Undergraduate'
            when 4 then 'Graduate'
            when 21 then 'Undergrad degree-seeking'
            when 22 then 'Undergrad non-degree-seeking'
            else 'Other'
        end as student_level_name,
        
        -- Total enrollment (all genders) - cast to handle large values
        efytotlt::number(10,0) as total_enrollment,
        efytotlm::number(10,0) as total_enrollment_men,
        efytotlw::number(10,0) as total_enrollment_women,
        
        -- American Indian or Alaska Native (critical for HB8 equity metrics)
        efyaiant::number(10,0) as amer_indian_total,
        efyaianm::number(10,0) as amer_indian_men,
        efyaianw::number(10,0) as amer_indian_women,
        
        -- Asian
        efyasiat::number(10,0) as asian_total,
        efyasiam::number(10,0) as asian_men,
        efyasiaw::number(10,0) as asian_women,
        
        -- Black or African American (key HB8 equity population)
        efybkaat::number(10,0) as black_total,
        efybkaam::number(10,0) as black_men,
        efybkaaw::number(10,0) as black_women,
        
        -- Hispanic or Latino (largest minority group in Texas, critical for HB8)
        efyhispt::number(10,0) as hispanic_total,
        efyhispm::number(10,0) as hispanic_men,
        efyhispw::number(10,0) as hispanic_women,
        
        -- Native Hawaiian or Other Pacific Islander
        efynhpit::number(10,0) as native_hawaiian_total,
        efynhpim::number(10,0) as native_hawaiian_men,
        efynhpiw::number(10,0) as native_hawaiian_women,
        
        -- White
        efywhitt::number(10,0) as white_total,
        efywhitm::number(10,0) as white_men,
        efywhitw::number(10,0) as white_women,
        
        -- Two or more races
        efy2mort::number(10,0) as two_or_more_total,
        efy2morm::number(10,0) as two_or_more_men,
        efy2morw::number(10,0) as two_or_more_women,
        
        -- Race/ethnicity unknown
        efyunknt::number(10,0) as race_unknown_total,
        efyunknm::number(10,0) as race_unknown_men,
        efyunknw::number(10,0) as race_unknown_women,
        
        -- U.S. Nonresident (international students)
        efynralt::number(10,0) as nonresident_total,
        efynralm::number(10,0) as nonresident_men,
        efynralw::number(10,0) as nonresident_women,
        
        -- New gender categories (IPEDS added these for inclusivity)
        efyguun::number(10,0) as gender_unknown,
        efyguan::number(10,0) as another_gender,
        efygutot::number(10,0) as gender_unknown_or_another_total,
        efygukn::number(10,0) as gender_binary_total  -- Total reported as men or women

    from source
)


select * from renamed