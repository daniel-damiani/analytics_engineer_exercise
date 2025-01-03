{{ config(materialized='incremental') }}

select * from {{ ref('source2_stage')}}