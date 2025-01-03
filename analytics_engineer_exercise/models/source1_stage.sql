{{ config(materialized='view') }}

with source as (
      select * from {{ source('raw_table', 'source1') }}
),
renamed as (
    select
        {{ adapter.quote("Name") }} as name,
        {{ adapter.quote("Credential Type") }} as credential_type,
        {{ adapter.quote("Credential Number") }} as credential_number,
        {{ adapter.quote("Status") }} as status,
        {{ adapter.quote("Expiration Date") }} as expiration_date,
        {{ adapter.quote("Disciplinary Action") }} as disciplinary_action,
        {{ adapter.quote("Address") }} as address,
        {{ adapter.quote("State") }} as state,
        {{ adapter.quote("County") }} as county,
        {{ adapter.quote("Phone") }} as phone,
        {{ adapter.quote("First Issue Date") }} as first_issue_date,
        {{ adapter.quote("Primary Contact Name") }} as primary_contact_name,
        {{ adapter.quote("Primary Contact Role") }} as primary_contact_role,
        current_timestamp as load_datetime
    from source
)
select * from renamed
  