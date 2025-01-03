{{ config(materialized='incremental') }}

select * from {{ ref('source1_stage')}}