{{ config(materialized='view') }}

select * from {{ source('staging','external_data')}}
limit 100