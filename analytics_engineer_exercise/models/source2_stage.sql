{{ config(materialized='view') }}

with source as (
      select * from {{ source('raw_table', 'source2') }}
),
renamed as (
    select
        {{ adapter.quote("Type License") }} as type_license,
        {{ adapter.quote("Company") }} as company,
        {{ adapter.quote("Accepts Subsidy") }} as accepts_subsidy,
        {{ adapter.quote("Year Round") }} as year_round,
        {{ adapter.quote("Daytime Hours") }} as daytime_hours,
        {{ adapter.quote("Star Level") }} as star_level,
        {{ adapter.quote("Mon") }} as monday,
        {{ adapter.quote("Tues") }} as tuesday,
        {{ adapter.quote("Wed") }} as wednesday,
        {{ adapter.quote("Thurs") }} as thursday,
        {{ adapter.quote("Friday") }} as friday,
        {{ adapter.quote("Saturday") }} as saturday,
        {{ adapter.quote("Sunday") }} as sunday,
        {{ adapter.quote("Primary Caregiver") }} as primary_caregiver,
        {{ adapter.quote("Phone") }} as phone,
        {{ adapter.quote("Email") }} as email,
        {{ adapter.quote("Address1") }} as address1,
        {{ adapter.quote("Address2") }} as address2,
        {{ adapter.quote("City") }} as city,
        {{ adapter.quote("State") }} as state,
        {{ adapter.quote("Zip") }} as zip,
        {{ adapter.quote("Subsidy Contract Number") }} as subsidy_contract_number,
        {{ adapter.quote("Total Cap") }} as total_cap,
        {{ adapter.quote("Ages Accepted 1") }} as ages_accepted_1,
        {{ adapter.quote("AA2") }} as ages_accepted_2,
        {{ adapter.quote("AA3") }} as ages_accepted_3,
        {{ adapter.quote("AA4") }} as ages_accepted_4,
        {{ adapter.quote("License Monitoring Since") }} as license_monitoring_since,
        {{ adapter.quote("School Year Only") }} as school_year_only,
        {{ adapter.quote("Evening Hours") }} as evening_hours,
        current_timestamp as load_datetime
    from source
)
select * from renamed
  