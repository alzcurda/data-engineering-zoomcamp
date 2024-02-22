{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata as (
    select *
    from {{ ref('stg_fhv_tripdata_2019') }}
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select fhv_tripdata.*
from fhv_tripdata
inner join dim_zones as pickup_zone
on fhv_tripdata.pulocationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_tripdata.dolocationid = dropoff_zone.locationid
