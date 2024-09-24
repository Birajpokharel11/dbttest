{{
  config(
    materialized='view'
  )
}}

with customers as (

    select
        id as customer_id
        ...

)
