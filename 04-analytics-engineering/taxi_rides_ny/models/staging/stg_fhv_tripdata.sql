with 

source as (

    select * from {{ source('staging', 'raw_fhv_tripdata') }}

),

renamed as (

    select
        dispatching_base_num,
        pickup_datetime,
        dropoff_datetime,
        pulocationid,
        dolocationid,
        sr_flag,
        affiliated_base_number

    from source

)

select * from renamed
