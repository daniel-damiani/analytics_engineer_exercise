{{ config(materialized='view') }}

with source as (
      select * from {{ source('raw_table', 'salesforce_leads') }}
),
renamed as (
    /*
        Creating HK, HD, and effective_from to track slowly changing dimmension.
    */
    select
        CAST(MD5("id"::TEXT) AS BYTEA) AS leads_HK,
        CAST(MD5(
        CONCAT_WS('||',
        NULLIF(UPPER(TRIM("id"::TEXT)), ''),
        NULLIF(UPPER(TRIM("is_deleted"::TEXT)), ''),
        NULLIF(UPPER(TRIM("last_name"::TEXT)), ''),
        NULLIF(UPPER(TRIM("first_name"::TEXT)), ''),
        NULLIF(UPPER(TRIM("title"::TEXT)), ''),
        NULLIF(UPPER(TRIM("company"::TEXT)), ''),
        NULLIF(UPPER(TRIM("street"::TEXT)), ''),
        NULLIF(UPPER(TRIM("city"::TEXT)), ''),
        NULLIF(UPPER(TRIM("state"::TEXT)), ''),
        NULLIF(UPPER(TRIM("postal_code"::TEXT)), ''),
        NULLIF(UPPER(TRIM("country"::TEXT)), ''),
        NULLIF(UPPER(TRIM("phone"::TEXT)), ''),
        NULLIF(UPPER(TRIM("mobile_phone"::TEXT)), ''),
        NULLIF(UPPER(TRIM("email"::TEXT)), ''),
        NULLIF(UPPER(TRIM("website"::TEXT)), ''),
        NULLIF(UPPER(TRIM("lead_source"::TEXT)), ''),
        NULLIF(UPPER(TRIM("status"::TEXT)), ''),
        NULLIF(UPPER(TRIM("is_converted"::TEXT)), ''),
        NULLIF(UPPER(TRIM("created_date"::TEXT)), ''),
        NULLIF(UPPER(TRIM("last_modified_date"::TEXT)), ''),
        NULLIF(UPPER(TRIM("last_activity_date"::TEXT)), ''),
        NULLIF(UPPER(TRIM("last_viewed_date"::TEXT)), ''),
        NULLIF(UPPER(TRIM("last_referenced_date"::TEXT)), ''),
        NULLIF(UPPER(TRIM("email_bounced_reason"::TEXT)), ''),
        NULLIF(UPPER(TRIM("email_bounced_date"::TEXT)), ''),
        NULLIF(UPPER(TRIM("outreach_stage_c"::TEXT)), ''),
        NULLIF(UPPER(TRIM("current_enrollment_c"::TEXT)), ''),
        NULLIF(UPPER(TRIM("capacity_c"::TEXT)), ''),
        NULLIF(UPPER(TRIM("lead_source_last_updated_c"::TEXT)), ''),
        NULLIF(UPPER(TRIM("brightwheel_school_uuid_c"::TEXT)), '')
        )
        ) AS BYTEA) AS leads_HD,

        {{ adapter.quote("id") }},
        {{ adapter.quote("is_deleted") }},
        {{ adapter.quote("last_name") }},
        {{ adapter.quote("first_name") }},
        {{ adapter.quote("title") }},
        {{ adapter.quote("company") }},
        {{ adapter.quote("street") }},
        {{ adapter.quote("city") }},
        {{ adapter.quote("state") }},
        {{ adapter.quote("postal_code") }},
        {{ adapter.quote("country") }},
        {{ adapter.quote("phone") }},
        {{ adapter.quote("mobile_phone") }},
        {{ adapter.quote("email") }},
        {{ adapter.quote("website") }},
        {{ adapter.quote("lead_source") }},
        {{ adapter.quote("status") }},
        {{ adapter.quote("is_converted") }},
        {{ adapter.quote("created_date") }},
        {{ adapter.quote("last_modified_date") }},
        {{ adapter.quote("last_activity_date") }},
        {{ adapter.quote("last_viewed_date") }},
        {{ adapter.quote("last_referenced_date") }},
        {{ adapter.quote("email_bounced_reason") }},
        {{ adapter.quote("email_bounced_date") }},
        {{ adapter.quote("outreach_stage_c") }},
        {{ adapter.quote("current_enrollment_c") }},
        {{ adapter.quote("capacity_c") }},
        {{ adapter.quote("lead_source_last_updated_c") }},
        {{ adapter.quote("brightwheel_school_uuid_c") }},
        last_modified_date as effective_from,
        current_timestamp as load_datetime
    from source
),

deduplicate as (
    select *,
        ROW_NUMBER() OVER (
        PARTITION BY leads_HK, effective_from
        ORDER BY effective_from DESC) as rnum
    from renamed
)

select 
    leads_hk,	
    leads_hd,	
    id,	
    is_deleted,	
    last_name,	
    first_name,	
    title,	
    company,	
    street,	
    city,	
    state,	
    postal_code,	
    country,
    phone,
    mobile_phone,
    email,
    website,
    lead_source,
    status,
    is_converted,
    created_date,
    last_modified_date,
    last_activity_date,
    last_viewed_date,
    last_referenced_date,
    email_bounced_reason,
    email_bounced_date,
    outreach_stage_c,
    current_enrollment_c,
    capacity_c,
    lead_source_last_updated_c,
    brightwheel_school_uuid_c,
    effective_from,
    load_datetime
from deduplicate
where rnum = 1  