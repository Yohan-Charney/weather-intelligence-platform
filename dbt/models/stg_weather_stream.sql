{{ config(materialized='table') }}

with source_data as (

    select *
    from {{ source('weather_stream_us', 'raw_weather') }}

),

cleaned as (

    select
        -- Normalisation ville
        case 
            when ville = 'Lisbon' then 'Lisbonne'
            else ville
        end as ville,

        -- Timestamp réel de mesure (secondes -> timestamp)
        timestamp_seconds(time_api) as weather_ts,

        -- Timestamp ingestion (string -> timestamp)
        timestamp(heure_run) as run_ts,

        -- On prépare le "bucket" horaire (la clé de l'heure)
        timestamp_trunc(timestamp_seconds(time_api), hour) as heure_meteo,

        temperature,
        humidite,
        vitesse_vent,
        categorie_meteo,
        description_meteo

    from source_data
),

deduplicated_hourly as (

    select *
    from (
        select
            *,
            row_number() over (
                partition by ville, heure_meteo
                order by coalesce(run_ts, heure_meteo) desc
            ) as row_num
        from cleaned
    )
    where row_num = 1
)

select
    ville,
    heure_meteo,          -- 1 seule mesure par heure
    run_ts,
    temperature,
    humidite,
    vitesse_vent,
    categorie_meteo,
    description_meteo
from deduplicated_hourly
