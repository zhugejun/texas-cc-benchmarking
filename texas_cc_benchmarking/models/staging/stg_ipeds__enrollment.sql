-- stg_ipeds__enrollment.sql
-- enrollment by program, award level, and demographics

with source as (
    select * from {{ source ('raw_ipeds', 'effy_2024')}}
),

renamed as (
    select
        unitid,
        effyalev as student_level,

        case effyalev
            when 1 then 'All students'
            when 2 then 'Undergraduate'
            when 4 then 'Graduate'
            when 21 then 'Undergrad degree-seeking'
            when 22 then 'Undergrad non-degree-seeking'
            else 'Other'
        end as student_level_name,

        -- total enrollment
        efytotlt as total_enrollment,
        efytotlm as total_enrollment_men,
        efytotlw as total_enrollment_women,

        -- by race/ethnicity
        efyaiant as enrollment_amer_indian,
        efyasiat as enrollment_asian,
        efybkaat as enrollment_black,
        efyhispt as enrollment_hispanic,
        efynhpit as enrollment_native_hawaiian,
        efywhitt as enrollment_white,
        efy2mort as enrollment_two_or_more,
        efyunknt as enrollment_unknown_race,
        efynralt as enrollment_nonresident_alien

    from source
)


select * from renamed