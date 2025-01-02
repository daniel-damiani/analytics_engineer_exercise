with source as (
      select * from {{ source('sample_files', 'salesforce_leads') }}
),
renamed as (
    select
        

    from source
)
select * from renamed
  