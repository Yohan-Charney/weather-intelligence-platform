{{ config(
    materialized='table',
    alias='previsions_cible_24h'
) }}

-- Objectif :
-- Pour chaque ville et chaque créneau météo (date_heure_prevue),
-- sélectionne la prévision la plus proche de 24h avant.
-- (Fenêtre de tolérance : 18h à 30h, adaptée à une collecte toutes les 6h)

with previsions as (

    select *
    from {{ ref('stg_forecast') }}

),

classement as (

    select
        *,
        abs(horizon_heures - 24) as ecart_a_24h,
        row_number() over (
            partition by ville, date_heure_prevue
            order by abs(horizon_heures - 24) asc, date_heure_collecte desc
        ) as rn
    from previsions

)

select
    ville,
    date_heure_prevue,
    date_heure_collecte,
    horizon_heures,

    round(temperature_prevue,2) as temperature_prevue,
    round(humidite_prevue,2) as humidite_prevue,
    round(vent_prevu,2) as vent_prevu,
    categorie_prevue,
    description_prevue

from classement
where rn = 1
