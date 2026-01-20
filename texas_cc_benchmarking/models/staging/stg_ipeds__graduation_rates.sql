-- stg_ipeds__graduation_rates.sql
-- graduation rates by demographics

with source as (
    {{ dbt_utils.union_relations(
        relations=[
            source('raw_ipeds', 'gr_2020'),
            source('raw_ipeds', 'gr_2021'),
            source('raw_ipeds', 'gr_2022'),
            source('raw_ipeds', 'gr_2023'),
            source('raw_ipeds', 'gr_2024'),
        ]
    ) }}
),

renamed as (
    select 
        unitid,
          
          -- Cohort identifying information (CRITICAL for understanding the data)
          grtype as cohort_type,
          case grtype
              -- 4-year institutions
              when 1 then '4-year institutions'
              when 40 then 'Total exclusions 4-year schools'
              when 2 then 'Adjusted cohort (revised cohort minus exclusions)'
              when 3 then 'Completers within 150% of normal time'
              when 4 then 'Transfer-out students'
              when 41 then 'Still enrolled'
              when 42 then 'No longer enrolled'
              -- Bachelor's subcohort (4-year)
              when 6 then 'Bachelor degree-seeking subcohort (4-year)'
              when 7 then 'Bachelor subcohort exclusions (4-year)'
              when 8 then 'Adjusted cohort (revised cohort minus exclusions)'
              when 9 then 'Completers within 150% of normal time total'
              when 10 then 'Completers of programs < 2 years (150% time)'
              when 11 then 'Completers of programs 2 to <4 years (150% time)'
              when 12 then 'Completers of bachelor degrees total (150% time)'
              when 13 then 'Completers of bachelor degrees in 4 years or less'
              when 14 then 'Completers of bachelor degrees in 5 years'
              when 15 then 'Completers of bachelor degrees in 6 years'
              when 16 then 'Transfer-out students'
              when 43 then 'Still enrolled'
              when 44 then 'No longer enrolled'
              -- Other degree-seeking subcohort (4-year)
              when 18 then 'Other degree-seeking subcohort (4-year)'
              when 19 then 'Other degree-seeking subcohort exclusions (4-year)'
              when 20 then 'Adjusted cohort (revised cohort minus exclusions)'
              when 21 then 'Completers within 150% of normal time total'
              when 22 then 'Completers of programs < 2 years (150% time)'
              when 23 then 'Completers of programs 2 to <4 years (150% time)'
              when 24 then 'Completers of bachelor degrees (150% time)'
              when 25 then 'Transfer-out students'
              when 45 then 'Still enrolled'
              when 46 then 'No longer enrolled'
              -- 2-YEAR INSTITUTIONS (Texas community colleges)
              when 27 then 'Degree/certificate-seeking students (2-year)'
              when 28 then 'Degree/certificate-seeking exclusions (2-year)'
              when 29 then 'Adjusted cohort (revised cohort minus exclusions) [2-YEAR]'
              when 30 then 'Completers within 150% of normal time total [2-YEAR]'
              when 31 then 'Completers of programs < 2 years (150% time) [2-YEAR]'
              when 32 then 'Completers of programs 2 to <4 years (150% time) [2-YEAR]'
              when 35 then 'Completers within 100% of normal time total [2-YEAR]'
              when 36 then 'Completers of programs < 2 years (100% time) [2-YEAR]'
              when 37 then 'Completers of programs 2 to <4 years (100% time) [2-YEAR]'
              when 33 then 'Transfer-out students [2-YEAR]'
              when 47 then 'Still enrolled [2-YEAR]'
              when 48 then 'No longer enrolled [2-YEAR]'
              else 'Other cohort type'
          end as cohort_type_description,
          
          chrtstat as cohort_status,
          case chrtstat
              when 10 then 'Revised cohort'
              when 11 then 'Exclusions'
              when 12 then 'Adjusted cohort (revised cohort minus exclusions)'
              when 13 then 'Completers within 150% of normal time'
              when 14 then 'Completers of programs < 2 years (150% time)'
              when 15 then 'Completers of programs 2 to <4 years (150% time)'
              when 16 then 'Completers of bachelor degrees (150% time)'
              when 17 then 'Completers of bachelor degrees in 4 years or less'
              when 18 then 'Completers of bachelor degrees in 5 years'
              when 19 then 'Completers of bachelor degrees in 6 years'
              when 20 then 'Transfer-out students'
              when 22 then 'Completers within 100% of normal time total'
              when 23 then 'Completers of programs < 2 years (100% time)'
              when 24 then 'Completers of programs 2 to <4 years (100% time)'
              when 31 then 'Still enrolled'
              when 32 then 'No longer enrolled'
              else 'Other status'
          end as cohort_status_description,
          
          section as data_section,
          cohort as cohort_year,
          line as survey_line,
          
          -- Grand totals (all genders) - cast to handle large cohorts
          grtotlt::number(10,0) as total,
          grtotlm::number(10,0) as total_men,
          grtotlw::number(10,0) as total_women,

          -- American Indian or Alaska Native (critical for HB8 equity metrics)
          graiant::number(10,0) as amer_indian_total,
          graianm::number(10,0) as amer_indian_men,
          graianw::number(10,0) as amer_indian_women,

          -- Asian
          grasiat::number(10,0) as asian_total,
          grasiam::number(10,0) as asian_men,
          grasiaw::number(10,0) as asian_women,

          -- Black or African American (key HB8 equity population)
          grbkaat::number(10,0) as black_total,
          grbkaam::number(10,0) as black_men,
          grbkaaw::number(10,0) as black_women,

          -- Hispanic or Latino (CRITICAL for Texas community colleges)
          grhispt::number(10,0) as hispanic_total,
          grhispm::number(10,0) as hispanic_men,
          grhispw::number(10,0) as hispanic_women,

          -- Native Hawaiian or Other Pacific Islander
          grnhpit::number(10,0) as native_hawaiian_total,
          grnhpim::number(10,0) as native_hawaiian_men,
          grnhpiw::number(10,0) as native_hawaiian_women,

          -- White
          grwhitt::number(10,0) as white_total,
          grwhitm::number(10,0) as white_men,
          grwhitw::number(10,0) as white_women,

          -- Two or more races
          gr2mort::number(10,0) as two_or_more_total,
          gr2morm::number(10,0) as two_or_more_men,
          gr2morw::number(10,0) as two_or_more_women,

          -- Race/ethnicity unknown
          grunknt::number(10,0) as race_unknown_total,
          grunknm::number(10,0) as race_unknown_men,
          grunknw::number(10,0) as race_unknown_women,

          -- U.S. Nonresident (international students)
          grnralt::number(10,0) as nonresident_total,
          grnralm::number(10,0) as nonresident_men,
          grnralw::number(10,0) as nonresident_women
        
    from source
    where year = 2024
)


select * from renamed