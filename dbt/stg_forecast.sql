{{ config(
    materialized='table',
    alias='previsions_meteo_nettoyees'

) }}


-- Objectif :
-- Nettoyer les prévisions météo (forecast), reconstruire les timestamps,
-- supprimer les doublons stricts, et calculer l'horizon de prévision en heures.

with donnees_source as (

    select *
    from {{ source('weather_stream_us', 'raw_forecast') }}

),

nettoyage as (

    select
        -- 1) Ville : harmonisation (ex : Lisbon -> Lisbonne)
        case
            when ville = 'Lisbon' then 'Lisbonne'
            else ville
        end as ville,

        -- 2) Date/heure de collecte (run) : quand notre pipeline a appelé l’API
        safe_cast(heure_run as timestamp) as date_heure_collecte,

        -- 3) Date/heure prévue (forecast) : créneau météo prévu
        -- On utilise plusieurs sources possibles car certains champs sont parfois NULL.
        coalesce(
            timestamp_seconds(safe_cast(forecast_timestamp as int64)),
            timestamp_seconds(safe_cast(time_api as int64)),
            safe_cast(split(record_forecast_id, '_')[offset(1)] as timestamp)
        ) as date_heure_prevue,

        -- 4) Mesures prévues
        temperature as temperature_prevue,
        humidite as humidite_prevue,
        vitesse_vent as vent_prevu,
        categorie_meteo as categorie_prevue,
        description_meteo as description_prevue,

        -- 5) Identifiant : si manquant, on le reconstruit
        coalesce(
            record_forecast_id,
            concat(
                case when ville = 'Lisbon' then 'Lisbonne' else ville end,
                '_',
                format_timestamp('%Y-%m-%dT%H:%M:%S%Ez', coalesce(
                    timestamp_seconds(safe_cast(forecast_timestamp as int64)),
                    timestamp_seconds(safe_cast(time_api as int64))
                ))
            )
        ) as id_prevision

    from donnees_source
),

dedup_doublons_stricts as (

    -- On enlève uniquement les doublons "copier-coller" :
    -- mêmes valeurs + même ville + même date prévue + même date de collecte
    select *
    from (
        select
            *,
            row_number() over (
                partition by
                    ville, date_heure_collecte, date_heure_prevue
                order by id_prevision desc
            ) as rn
        from nettoyage
        where date_heure_collecte is not null
          and date_heure_prevue is not null
    )
    where rn = 1

)

select
    ville,
    id_prevision,
    date_heure_collecte,
    date_heure_prevue,

    -- Horizon : combien d'heures avant l'évènement la prévision a été récupérée
    timestamp_diff(date_heure_prevue, date_heure_collecte, hour) as horizon_heures,

    temperature_prevue,
    humidite_prevue,
    vent_prevu,
    categorie_prevue,
    description_prevue

from dedup_doublons_stricts