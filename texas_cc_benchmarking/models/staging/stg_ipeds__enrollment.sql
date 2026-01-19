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
        
        -- Total enrollment (all genders)
        efytotlt as total_enrollment,
        efytotlm as total_enrollment_men,
        efytotlw as total_enrollment_women,
        
        -- American Indian or Alaska Native (critical for HB8 equity metrics)
        efyaiant as amer_indian_total,
        efyaianm as amer_indian_men,
        efyaianw as amer_indian_women,
        
        -- Asian
        efyasiat as asian_total,
        efyasiam as asian_men,
        efyasiaw as asian_women,
        
        -- Black or African American (key HB8 equity population)
        efybkaat as black_total,
        efybkaam as black_men,
        efybkaaw as black_women,
        
        -- Hispanic or Latino (largest minority group in Texas, critical for HB8)
        efyhispt as hispanic_total,
        efyhispm as hispanic_men,
        efyhispw as hispanic_women,
        
        -- Native Hawaiian or Other Pacific Islander
        efynhpit as native_hawaiian_total,
        efynhpim as native_hawaiian_men,
        efynhpiw as native_hawaiian_women,
        
        -- White
        efywhitt as white_total,
        efywhitm as white_men,
        efywhitw as white_women,
        
        -- Two or more races
        efy2mort as two_or_more_total,
        efy2morm as two_or_more_men,
        efy2morw as two_or_more_women,
        
        -- Race/ethnicity unknown
        efyunknt as race_unknown_total,
        efyunknm as race_unknown_men,
        efyunknw as race_unknown_women,
        
        -- U.S. Nonresident (international students)
        efynralt as nonresident_total,
        efynralm as nonresident_men,
        efynralw as nonresident_women,
        
        -- New gender categories (IPEDS added these for inclusivity)
        efyguun as gender_unknown,
        efyguan as another_gender,
        efygutot as gender_unknown_or_another_total,
        efygukn as gender_binary_total  -- Total reported as men or women

    from source
    where year = 2024
)


select * from renamed