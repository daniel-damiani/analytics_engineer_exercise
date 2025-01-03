with latest_records as (
select *,
    ROW_NUMBER() OVER (
    PARTITION BY leads_HK, effective_from
    ORDER BY effective_from DESC) as rnum
FROM {{ ref('leads_raw') }}
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
        split_part(initcap(street),',',1)::varchar as street,
        initcap(city)::varchar as city,
    (CASE
    WHEN LENGTH(TRIM(state)) = 2 THEN UPPER(state)
    ELSE INITCAP(state)
    END)::varchar AS state, /* could use more cleaning/standardizing */
    postal_code,	
    country,
    REGEXP_REPLACE(phone::varchar, '[^0-9]+', '', 'g')::varchar as phone,
    REGEXP_REPLACE(mobile_phone::varchar, '[^0-9]+', '', 'g')::varchar as phone2,
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
FROM latest_records
where rnum = 1
