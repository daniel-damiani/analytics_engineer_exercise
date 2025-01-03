{{ config(materialized='incremental') }}

select * from {{ ref('source3_stage')}}