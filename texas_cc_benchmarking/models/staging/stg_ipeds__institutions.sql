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
      instnm as institution_name,
      ialias as institution_alias,
      year,

      -- location
      city,
      stabbr as state,
      zip,
      fips as fips_code,
      obereg as bureau_region,

      --classification
      sector,
      case sector
          when 1 then 'Public 4-year'
          when 2 then 'Private nonprofit 4-year'
          when 3 then 'Private for-profit 4-year'
          when 4 then 'Public 2-year'
          when 5 then 'Private nonprofit 2-year'
          when 6 then 'Private for-profit 2-year'
          when 7 then 'Public less-than-2-year'
          when 8 then 'Private nonprofit less-than-2-year'
          when 9 then 'Private for-profit less-than-2-year'
          else 'Unknown'
      end as sector_name,

      iclevel as institution_level,
      case iclevel
          when 1 then '4-year or above'
          when 2 then '2-year'
          when 3 then 'Less-than-2-year'
          else 'Unknown'
      end as institution_level_name,

      control,
      case control
          when 1 then 'Public'
          when 2 then 'Private nonprofit'
          when 3 then 'Private for-profit'
          else 'Unknown'
      end as control_name,

      -- other attributes
      hloffer as highest_level_offering,
      ugoffer as undergraduate_offering,
      groffer as graduate_offering,

      locale,
      case when
        locale in (11, 12, 13) then 'City'
        when locale in (21, 22, 23) then 'Suburb'
        when locale in (31, 32, 33) then 'Town'
        when locale in (41, 42, 43) then 'Rural'
        else 'Unknown'
      end as locale_type,

      instsize as institution_size,

      webaddr as website_url,
      adminurl as admissions_url,
      faidurl as financial_aid_url,
      applurl as application_url

    from source
    where year = 2024
)



select * from renamed
