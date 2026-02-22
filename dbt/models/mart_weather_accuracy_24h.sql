{{ config(
    materialized='table',
    alias='fiabilite_previsions_24h'
) }}

-- Objectif :
-- 1) Aligner l'observé (mart_weather_stream) sur le même grain que le forecast (créneaux de 3h)
-- 2) Joindre avec les prévisions sélectionnées à ~24h (previsions_cible_24h)
-- 3) Calculer un score de fiabilité + KPI de disponibilité des prévisions

with observe_source as (

    -- Table déjà prête (jour + heure séparés)
    select
        ville,
        jour,
        heure,
        temperature as temperature_reelle_h,
        vitesse_vent as vent_reel_h,
        humidite as humidite_reelle_h,
        categorie_meteo as categorie_reelle_h,
        description_meteo as description_reelle_h,
        indice_risque_meteo,
        niveau_risque
    from {{ ref('mart_weather_stream') }}

),

observe_horodatage as (

    -- On reconstruit un timestamp à partir de jour + heure
    select
        ville,
        timestamp(datetime(jour, time(heure, 0, 0))) as date_heure_h,
        temperature_reelle_h,
        vent_reel_h,
        humidite_reelle_h,
        categorie_reelle_h,
        description_reelle_h
    from observe_source

),

observe_3h as (

    -- On regroupe l'observé en créneaux de 3h : 00, 03, 06, 09, 12, 15, 18, 21
    select
        ville,

        -- slot 3h : on "arrondit vers le bas" au multiple de 3h
        timestamp_seconds(div(unix_seconds(date_heure_h), 10800) * 10800) as date_heure,

        round(avg(temperature_reelle_h),2) as temperature_reelle,
        round(avg(vent_reel_h),2) as vent_reel,
        round(avg(humidite_reelle_h),2) as humidite_reelle,

        any_value(categorie_reelle_h) as categorie_reelle,
        any_value(description_reelle_h) as description_reelle

    from observe_horodatage
    group by ville, date_heure

),

previsions_24h as (

    -- Prévision unique par ville + date_heure_prevue, choisie au plus proche de 24h
    select
        ville,
        date_heure_prevue as date_heure,

        date_heure_collecte,
        horizon_heures,

        temperature_prevue,
        vent_prevu,
        humidite_prevue,
        categorie_prevue,
        description_prevue

    from {{ ref('int_forecast_24h') }}

),

fusion as (

    -- On veut voir les créneaux observés même si la prévision manque :
    -- donc base = observe_3h, puis left join prévisions
    select
        o.ville,
        o.date_heure,

        p.date_heure_collecte,
        p.horizon_heures,

        -- Prévu vs Réel
        p.temperature_prevue,
        o.temperature_reelle,

        p.vent_prevu,
        o.vent_reel,

        p.humidite_prevue,
        o.humidite_reelle,

        p.categorie_prevue,
        o.categorie_reelle,

        p.description_prevue,
        o.description_reelle

    from observe_3h o
    left join previsions_24h p
      on p.ville = o.ville
     and p.date_heure = o.date_heure

),

scores as (

    select
        *,

        -- KPI couverture : a-t-on une prévision 24h pour ce créneau ?
        case
            when date_heure_collecte is not null then 1 else 0
        end as prevision_disponible_24h,

        -- KPI comparabilité : a-t-on à la fois prévu ET observé ?
        case
            when temperature_reelle is not null
             and vent_reel is not null
             and humidite_reelle is not null
             and temperature_prevue is not null
             and vent_prevu is not null
             and humidite_prevue is not null
            then 1 else 0
        end as comparaison_possible,

        -- Écarts (NULL si pas de prévision ou pas d'observation)
        case when temperature_prevue is null or temperature_reelle is null
            then null else round(abs(temperature_prevue - temperature_reelle),2) end as ecart_temperature,

        case when vent_prevu is null or vent_reel is null
            then null else round(abs(vent_prevu - vent_reel),2) end as ecart_vent,

        case when humidite_prevue is null or humidite_reelle is null
            then null else round(abs(humidite_prevue - humidite_reelle),2) end as ecart_humidite,

        -- Score température (tes seuils)
        case
            when temperature_prevue is null or temperature_reelle is null then null
            when abs(temperature_prevue - temperature_reelle) <= 1 then 3
            when abs(temperature_prevue - temperature_reelle) <= 2 then 2
            when abs(temperature_prevue - temperature_reelle) <= 3 then 1
            else 0
        end as score_temperature_0_3,

        -- Score vent (m/s)
        case
            when vent_prevu is null or vent_reel is null then null
            when abs(vent_prevu - vent_reel) <= 1 then 3
            when abs(vent_prevu - vent_reel) <= 2 then 2
            when abs(vent_prevu - vent_reel) <= 3.5 then 1
            else 0
        end as score_vent_0_3,

        -- Score humidité (points %)
        case
            when humidite_prevue is null or humidite_reelle is null then null
            when abs(humidite_prevue - humidite_reelle) <= 5 then 3
            when abs(humidite_prevue - humidite_reelle) <= 10 then 2
            when abs(humidite_prevue - humidite_reelle) <= 20 then 1
            else 0
        end as score_humidite_0_3

    from fusion

)

select
    ville,
    date_heure,

    -- pratique BI
    date(date_heure) as jour,
    extract(hour from date_heure) as heure,
    cast(div(extract(hour from date_heure), 3) as int64) as tranche_3h_index,

    -- couverture / comparabilité
    prevision_disponible_24h,
    comparaison_possible,

    -- infos prévision sélectionnée 24h
    date_heure_collecte,
    horizon_heures,

    -- valeurs
    temperature_prevue,
    temperature_reelle,
    ecart_temperature,
    score_temperature_0_3,

    vent_prevu,
    vent_reel,
    ecart_vent,
    score_vent_0_3,

    humidite_prevue,
    humidite_reelle,
    ecart_humidite,
    score_humidite_0_3,

    categorie_prevue,
    categorie_reelle,
    description_prevue,
    description_reelle,

    -- Score global sur 100
    case
        when score_temperature_0_3 is null
          or score_vent_0_3 is null
          or score_humidite_0_3 is null
        then null
        else round((score_temperature_0_3 + score_vent_0_3 + score_humidite_0_3) / 9.0 * 100, 1)
    end as score_fiabilite_0_100,

    case
        when score_temperature_0_3 is null
          or score_vent_0_3 is null
          or score_humidite_0_3 is null
        then 'incomplet'
        when (score_temperature_0_3 + score_vent_0_3 + score_humidite_0_3) >= 8 then 'très fiable'
        when (score_temperature_0_3 + score_vent_0_3 + score_humidite_0_3) >= 6 then 'fiable'
        when (score_temperature_0_3 + score_vent_0_3 + score_humidite_0_3) >= 4 then 'assez fiable'
        else 'pas fiable'
    end as niveau_fiabilite

from scores
where date_heure_collecte is not null
