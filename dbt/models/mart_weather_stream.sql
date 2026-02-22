{{ config(materialized='table') }}

with base as (

    select *
    from {{ ref('stg_weather_stream') }}

),

scored as (

    select
        ville,
        heure_meteo,
        date(heure_meteo) as jour,
        extract(hour from heure_meteo) as heure,

        temperature,
        humidite,
        vitesse_vent,

        -- Catégorie d'origine (anglais) pour les règles
        categorie_meteo,
        description_meteo,

        -- Indice de risque météo (0/1/2) orienté logistique
        case
            -- 2 = Risque fort / dangereux
            when temperature < -10 then 2
            when temperature > 45 then 2
            when categorie_meteo= 'Snow' then 2
            when categorie_meteo= 'Thunderstorm' then 2
            when vitesse_vent >= 20 then 2  -- ~72 km/h

            -- 1 = Risque modéré
            when temperature >= 35 and temperature <= 45 then 1
            when temperature >= -10 and temperature < -5 then 1
            when categorie_meteo in ('Rain', 'Drizzle') then 1
            when vitesse_vent >= 17 then 1  -- ~61 km/h

            -- 0 = Aucun risque
            else 0
        end as indice_risque_meteo

    from base

)

select
    ville,
    heure_meteo,
    jour,
    heure,

    temperature,
    humidite,
    vitesse_vent,

    -- Catégorie météo en français (affichage)
    case
        when categorie_meteo = 'Rain' then 'Pluie'
        when categorie_meteo = 'Clouds' then 'Nuageux'
        when categorie_meteo = 'Clear' then 'Ciel dégagé'
        when categorie_meteo = 'Snow' then 'Neige'
        when categorie_meteo = 'Thunderstorm' then 'Orage'
        when categorie_meteo = 'Drizzle' then 'Bruine'
        when categorie_meteo is null then 'Inconnu'
        else categorie_meteo
    end as categorie_meteo,

    description_meteo,

    indice_risque_meteo,

    case
        when indice_risque_meteo = 0 then 'Aucun risque'
        when indice_risque_meteo = 1 then 'Risque modéré'
        else 'Risque fort'
    end as niveau_risque

from scored
