-- stg_olist_geolocation: DEVIATION from the thin 1:1 staging convention used
-- elsewhere. The raw table has ~1M rows with many duplicate/near-duplicate
-- lat/lng rows per zip prefix — not a usable grain as-is, so this model
-- aggregates to one row per zip-code prefix before anything downstream joins to it.

with source as (

    select * from {{ source('olist_raw', 'olist_geolocation') }}

),

renamed as (

    select
        geolocation_zip_code_prefix::text   as geolocation_zip_code_prefix,
        geolocation_lat::numeric            as geolocation_lat,
        geolocation_lng::numeric            as geolocation_lng,
        lower(trim(geolocation_city))       as geolocation_city,
        upper(trim(geolocation_state))      as geolocation_state

    from source

),

aggregated as (

    select
        geolocation_zip_code_prefix,
        avg(geolocation_lat)                                as geolocation_lat,
        avg(geolocation_lng)                                as geolocation_lng,
        mode() within group (order by geolocation_city)     as geolocation_city,
        mode() within group (order by geolocation_state)    as geolocation_state

    from renamed
    group by geolocation_zip_code_prefix

)

select * from aggregated
