WITH source1_data AS (
    SELECT
        null::varchar as accepts_financial_aid,
        null::varchar as ages_served,
        null::int as capacity,
        expiration_date::date as certificate_expiration_date,
        null::varchar as city,
        split_part(initcap(address),',',1)::varchar as address1,
        null::varchar as address2,
        initcap(name)::varchar as company,
        REGEXP_REPLACE(phone, '[^0-9]+', '', 'g')::varchar as phone,
        null::varchar as phone2,
        initcap(county)::varchar as county,
        null::varchar as curriculum_type,
        null::varchar as email,
        initcap(CASE
            WHEN primary_contact_name LIKE '%,%' THEN -- If there's a comma, split by it and take the first part
            split_part(primary_contact_name, ',', 1)
            WHEN primary_contact_name LIKE '%&%' THEN -- If there's an '&', split by it and take the first part
            split_part(primary_contact_name, '&', 1)
            WHEN primary_contact_name LIKE '% %' THEN -- If there are spaces (standard format), take the first word
            split_part(primary_contact_name, ' ', 1)
            ELSE -- Otherwise, the whole name is the first name
            primary_contact_name
        END)::varchar AS first_name,
        null::varchar as language,
        initcap(CASE
            WHEN primary_contact_name LIKE '%,%' THEN -- If there's a comma, split by it and take the last part
            split_part(primary_contact_name, ',', array_length(string_to_array(primary_contact_name, ','), 1))
            WHEN primary_contact_name LIKE '%&%' THEN -- If there's an '&', split by it and take the last part
            split_part(primary_contact_name, '&', array_length(string_to_array(primary_contact_name, '&'), 1))
            WHEN primary_contact_name LIKE '% %' THEN -- If there are spaces (standard format), take the last word
            split_part(primary_contact_name, ' ', array_length(string_to_array(primary_contact_name, ' '), 1))
            ELSE -- Otherwise, the whole name is considered the last name
            primary_contact_name
        END)::varchar AS last_name,
        status::varchar as license_status,
        TO_DATE(TRIM(nullif(first_issue_date,'')), 'MM/DD/YY') as license_issued,
        REGEXP_REPLACE(credential_number, '[^0-9]', '', 'g')::int as license_number,
        null::date as license_renewed,
        initcap(credential_type)::varchar as license_type,
        null::varchar as licensee_name,
        null::int as max_age,
        null::int as min_age,
        null::varchar as operator,
        null::varchar as provider_id,
        null::varchar as schedule,
        (CASE
        WHEN LENGTH(TRIM(state)) = 2 THEN UPPER(state)
        ELSE INITCAP(state)
        END)::varchar AS state, /* could use more cleaning/standardizing */
        initcap(primary_contact_role)::varchar as title,
        null::varchar as website_address,
        (REGEXP_MATCH(address, '\s(\d{5})(?:-\d{4})?$'))[1]::varchar AS zip,
        initcap(credential_type)::varchar as facility_type
    FROM {{ ref('source1_raw') }}
),
source2_data AS (
    SELECT
        accepts_subsidy::varchar as accepts_financial_aid,
        concat(
        case 
            when lower(coalesce(nullif(ages_accepted_1,''),nullif(ages_accepted_2,''),nullif(ages_accepted_3,''),nullif(ages_accepted_4,''))) like 'infant%' then '0'
            when lower(coalesce(nullif(ages_accepted_1,''),nullif(ages_accepted_2,''),nullif(ages_accepted_3,''),nullif(ages_accepted_4,''))) like 'toddler%' then '1'
            when lower(coalesce(nullif(ages_accepted_1,''),nullif(ages_accepted_2,''),nullif(ages_accepted_3,''),nullif(ages_accepted_4,''))) like 'preschool%' then '2'
            when lower(coalesce(nullif(ages_accepted_1,''),nullif(ages_accepted_2,''),nullif(ages_accepted_3,''),nullif(ages_accepted_4,''))) like 'school-age%' then '5' 
            else null 
        end,
        ' to ',
        case 
            when lower(coalesce(nullif(ages_accepted_4,''),nullif(ages_accepted_3,''),nullif(ages_accepted_2,''),nullif(ages_accepted_1,''))) like 'infant%' then '1'
            when lower(coalesce(nullif(ages_accepted_4,''),nullif(ages_accepted_3,''),nullif(ages_accepted_2,''),nullif(ages_accepted_1,''))) like 'toddler%' then '2'
            when lower(coalesce(nullif(ages_accepted_4,''),nullif(ages_accepted_3,''),nullif(ages_accepted_2,''),nullif(ages_accepted_1,''))) like 'preschool%' then '4'
            when lower(coalesce(nullif(ages_accepted_4,''),nullif(ages_accepted_3,''),nullif(ages_accepted_2,''),nullif(ages_accepted_1,''))) like 'school-age%' then '5+' 
            else null
        end,
        ' Years')::varchar as ages_served,
        total_cap::int as capacity,
        null::date as certificate_expiration_date,
        initcap(city)::varchar as city,
        initcap(address1)::varchar as address1,
        initcap(address2)::varchar as address2,
        initcap(company)::varchar as company,
        REGEXP_REPLACE(phone, '[^0-9]+', '', 'g')::varchar as phone,
        null::varchar as phone2,
        null::varchar as county,
        null::varchar as curriculum_type,
        lower(email)::varchar as email,
        split_part(split_part(primary_caregiver,'  ',1),' ',1)::varchar as first_name,
        null::varchar as language,
        split_part(split_part(primary_caregiver,'  ',1),' ',2)::varchar as last_name,
        null::varchar as license_status,
        to_date(REGEXP_REPLACE(license_monitoring_since, '[^0-9]+', '', 'g')::text,'YYYYMMDD') as license_issued,
        upper(split_part(type_license,' - K',2))::int as license_number,
        null::date as license_renewed,
        initcap(split_part(type_license,' - ',1))::varchar as license_type,
        null::varchar as licensee_name,
        (case 
            when lower(coalesce(nullif(ages_accepted_4,''),nullif(ages_accepted_3,''),nullif(ages_accepted_2,''),nullif(ages_accepted_1,''))) like 'infant%' then 1
            when lower(coalesce(nullif(ages_accepted_4,''),nullif(ages_accepted_3,''),nullif(ages_accepted_2,''),nullif(ages_accepted_1,''))) like 'toddler%' then 2
            when lower(coalesce(nullif(ages_accepted_4,''),nullif(ages_accepted_3,''),nullif(ages_accepted_2,''),nullif(ages_accepted_1,''))) like 'preschool%' then 4
            when lower(coalesce(nullif(ages_accepted_4,''),nullif(ages_accepted_3,''),nullif(ages_accepted_2,''),nullif(ages_accepted_1,''))) like 'school-age%' then null 
            else null 
        end)::int as max_age,
        (case 
            when lower(coalesce(nullif(ages_accepted_1,''),nullif(ages_accepted_2,''),nullif(ages_accepted_3,''),nullif(ages_accepted_4,''))) like 'infant%' then 0
            when lower(coalesce(nullif(ages_accepted_1,''),nullif(ages_accepted_2,''),nullif(ages_accepted_3,''),nullif(ages_accepted_4,''))) like 'toddler%' then 1
            when lower(coalesce(nullif(ages_accepted_1,''),nullif(ages_accepted_2,''),nullif(ages_accepted_3,''),nullif(ages_accepted_4,''))) like 'preschool%' then 2
            when lower(coalesce(nullif(ages_accepted_1,''),nullif(ages_accepted_2,''),nullif(ages_accepted_3,''),nullif(ages_accepted_4,''))) like 'school-age%' then 5 
            else null 
        end)::int as min_age,        
        null::varchar as operator,
        null::varchar as provider_id,
        null::varchar as schedule,
        (CASE
        WHEN LENGTH(TRIM(state)) = 2 THEN UPPER(state)
        ELSE INITCAP(state)
        END)::varchar AS state, /* could use more cleaning/standardizing */
        initcap(split_part(primary_caregiver,'  ',2))::varchar title,
        null::varchar as website_address,
        zip::varchar as zip,
        null::varchar as facility_type
    FROM {{ ref('source2_raw') }}
),
source3_data AS (
    SELECT
        null::varchar as accepts_financial_aid,
        (CASE
            WHEN COALESCE(infant, toddler, preschool, school) = 'N' THEN NULL
            ELSE
                CONCAT(
                    UPPER(
                        COALESCE(
                            CASE WHEN infant = 'Y' THEN '0' ELSE NULL END,
                            CASE WHEN toddler = 'Y' THEN '1' ELSE NULL END,
                            CASE WHEN preschool = 'Y' THEN '2' ELSE NULL END,
                            CASE WHEN school = 'Y' THEN '5' ELSE NULL END
                        )
                    ),
                    ' to ',
                    UPPER(
                        COALESCE(
                            CASE WHEN school = 'Y' THEN '5+' ELSE NULL END,
                            CASE WHEN preschool = 'Y' THEN '4' ELSE NULL END,
                            CASE WHEN toddler = 'Y' THEN '2' ELSE NULL END,
                            CASE WHEN infant = 'Y' THEN '1' ELSE NULL END
                        )
                    ),
                    ' Years'
                )
        END)::varchar AS ages_served,
        capacity::int as capacity,
        null::date as certificate_expiration_date,
        initcap(city)::varchar as city,
        initcap(address)::varchar as address1,
        null::varchar as address2,
        operation_name::varchar as company,
        REGEXP_REPLACE(phone, '[^0-9]+', '', 'g')::varchar as phone,
        null::varchar as phone2,
        initcap(county)::varchar as county,
        null::varchar as curriculum_type,
        lower(email_address)::varchar as email,
        null::varchar as first_name,
        null::varchar as language,
        null::varchar as last_name,
        status::varchar as license_status,
        TO_DATE(TRIM(nullif(issue_date,'')), 'MM/DD/YY') AS license_issued,
        null::int as license_number,
        null::date as license_renewed,
        initcap(type)::varchar as license_type,
        null::varchar as licensee_name,
        (case when school = 'Y' then null else
        COALESCE(
            CASE WHEN school = 'Y' THEN NULL::int END,
            CASE WHEN preschool = 'Y' THEN 4 ELSE NULL END,
            CASE WHEN toddler = 'Y' THEN 2 ELSE NULL END,
            CASE WHEN infant = 'Y' THEN 1 ELSE NULL END
        ) end)::int
        as max_age,
        COALESCE(
            CASE WHEN infant = 'Y' THEN 0 ELSE NULL END,
            CASE WHEN toddler = 'Y' THEN 1 ELSE NULL END,
            CASE WHEN preschool = 'Y' THEN 2 ELSE NULL END,
            CASE WHEN school = 'Y' THEN 5 ELSE NULL END
        )::int
        as min_age,
        null::varchar as operator,
        facility_id::varchar as provider_id,
        null::varchar as schedule,
        (CASE
        WHEN LENGTH(TRIM(state)) = 2 THEN UPPER(state)
        ELSE INITCAP(state)
        END)::varchar AS state, /* could use more cleaning/standardizing */
        null::varchar as title,
        null::varchar as website_address,
        zip::varchar as zip,
        null::varchar as facility_type
    FROM {{ ref('source3_raw') }}
)
-- Combining all source data
SELECT * FROM source1_data
UNION ALL
SELECT * FROM source2_data
UNION ALL
SELECT * FROM source3_data