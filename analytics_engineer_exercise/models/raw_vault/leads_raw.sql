{{ config(
    materialized='incremental',
    unique_key=(['leads_HK', 'effective_from'])
    ) }}

select * from {{ ref('leads_stage')}}
where leads_HK is not null