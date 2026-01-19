-- stg_ipdes__institutions.sql
-- institution-level data


with source as (
    {{ dbt_utils.union_relations(
        relations=[
            source('raw_ipeds', 'hd_2020'),
            source('raw_ipeds', 'hd_2021'),
            source('raw_ipeds', 'hd_2022'),
            source('raw_ipeds', 'hd_2023'),
            source('raw_ipeds', 'hd_2024'),
        ]
    ) }}
),

renamed as (
    select 
      unitid,
        opeid as ope_id,  -- Federal OPE ID (important for linking to federal data)
        instnm as institution_name,
        ialias as institution_alias,
        year,
        
        -- Location - Basic
        addr as street_address,
        city,
        stabbr as state_code,
        zip,
        fips as fips_state_code,
        obereg as bea_region,
        
        -- Location - Geographic Detail (important for peer grouping)
        countycd as county_fips_code,
        countynm as county_name,
        cbsa as cbsa_code,
        cbsatype as cbsa_type,  -- Metropolitan or Micropolitan
        csa as csa_code,
        cngdstcd as congressional_district_id,
        longitud as longitude,
        latitude as latitude,
        
        -- Locale (urban/rural classification - critical for HB8 peer groups)
        locale,
        case 
            when locale in (11, 12, 13) then 'City'
            when locale in (21, 22, 23) then 'Suburb'
            when locale in (31, 32, 33) then 'Town'
            when locale in (41, 42, 43) then 'Rural'
            else 'Unknown'
        end as locale_type,
        case 
            when locale in (11, 21, 31, 41) then 'Large'
            when locale in (12, 22, 32, 42) then 'Midsize'
            when locale in (13, 23, 33, 43) then 'Small'
            else 'Unknown'
        end as locale_size,
        
        -- Institutional Classification - CRITICAL for HB8 benchmarking
        sector,
        case sector
            when 1 then 'Public 4-year'
            when 2 then 'Private nonprofit 4-year'
            when 3 then 'Private for-profit 4-year'
            when 4 then 'Public 2-year'  -- TEXAS COMMUNITY COLLEGES
            when 5 then 'Private nonprofit 2-year'
            when 6 then 'Private for-profit 2-year'
            when 7 then 'Public less-than-2-year'
            when 8 then 'Private nonprofit less-than-2-year'
            when 9 then 'Private for-profit less-than-2-year'
            else 'Unknown'
        end as sector_name,
        
        iclevel as institution_level,
        case iclevel
            when 1 then 'Four or more years'
            when 2 then 'At least 2 but less than 4 years'
            when 3 then 'Less than 2 years'
            else 'Unknown'
        end as institution_level_name,
        
        control,
        case control
            when 1 then 'Public'
            when 2 then 'Private nonprofit'
            when 3 then 'Private for-profit'
            else 'Unknown'
        end as control_name,
        
        -- Carnegie Classifications (2021 and 2025 - most current)
        c21basic as carnegie_basic_2021,
        carnegieic as carnegie_institutional_classification_2025,
        carnegiesaec as carnegie_student_access_earnings_2025,
        carnegiersch as carnegie_research_activity_2025,
        carnegiesize as carnegie_size_2025,
        carnegiealf as carnegie_award_level_focus_2025,
        carnegieapm as carnegie_undergrad_program_mix_2025,
        carnegiegpm as carnegie_grad_program_mix_2025,
        
        -- Institutional Attributes
        hloffer as highest_level_offering,
        hdegofr1 as highest_degree_offered,
        ugoffer as undergrad_offering,
        groffer as graduate_offering,
        deggrant as degree_granting_status,
        
        -- Size
        instsize as institution_size_category,
        
        -- Special Designations
        landgrnt as land_grant_institution,
        hbcu as historically_black_college,
        tribal as tribal_college,
        hospital as has_hospital,
        medical as grants_medical_degree,
        
        -- System Membership (CRITICAL for Texas - many CCs are in multi-college districts)
        f1systyp as system_type,
        f1sysnam as system_name,
        f1syscod as system_id,
        
        -- Status and Eligibility
        act as institution_status,
        cyactive as active_current_year,
        opeflag as ope_title_iv_eligibility,
        closedat as date_closed,
        deathyr as year_deleted_from_ipeds,
        postsec as primarily_postsecondary,
        pseflag as postsecondary_institution_flag,
        pset4flg as postsecondary_title_iv_flag,
        
        -- Administrative
        chfnm as chief_administrator_name,
        chftitle as chief_administrator_title,
        gentele as general_phone,
        ein as employer_id_number,
        ueis as unique_entity_identifier,
        
        -- Reporting
        rptmth as reporting_method,
        instcat as institutional_category,
        dfrcgid as nces_comparison_group_id,
        dfrcuscg as custom_comparison_group_submitted,
        
        -- URLs
        webaddr as website_url,
        adminurl as admissions_url,
        faidurl as financial_aid_url,
        applurl as application_url,
        npricurl as net_price_calculator_url,
        veturl as veterans_tuition_url,
        athurl as athlete_graduation_rate_url,
        disaurl as disability_services_url
    from source
    where year = 2024
)



select * from renamed
