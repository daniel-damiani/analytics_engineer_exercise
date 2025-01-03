{{ config(materialized='view') }}

with source as (
      select * from {{ source('raw_table', 'source3') }}
),
renamed as (
    select
        {{ adapter.quote("Operation") }} as operation,
        {{ adapter.quote("Agency Number") }} as agency_number,
        {{ adapter.quote("Operation Name") }} as operation_name,
        {{ adapter.quote("Address") }} as address,
        {{ adapter.quote("City") }} as city,
        {{ adapter.quote("State") }} as state,
        {{ adapter.quote("Zip") }} as zip,
        {{ adapter.quote("County") }} as county,
        {{ adapter.quote("Phone") }} as phone,
        {{ adapter.quote("Type") }} as type,
        {{ adapter.quote("Status") }} as status,
        {{ adapter.quote("Issue Date") }} as issue_date,
        {{ adapter.quote("Capacity") }} as capacity,
        {{ adapter.quote("Email Address") }} as email_address,
        {{ adapter.quote("Facility ID") }} as facility_id,
        {{ adapter.quote("Monitoring Frequency") }} as monitoring_frequency,
        {{ adapter.quote("Infant") }} as infant,
        {{ adapter.quote("Toddler") }} as toddler,
        {{ adapter.quote("Preschool") }} as preschool,
        {{ adapter.quote("School") }} as school,
        current_timestamp as load_datetime
    from source
)
select * from renamed
  