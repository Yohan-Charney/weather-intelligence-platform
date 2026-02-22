{{ config(materialized='table') }}

-- Table de référence "actionnable" :
-- Pour chaque ville + catégorie météo prévue, calcule la fiabilité moyenne observée dans le passé.
-- Utilisée ensuite pour "noter" la confiance des prévisions futures (demain / après-demain).

with base as (

  select
    ville,
    categorie_prevue,
    score_fiabilite_0_100

  from {{ ref('mart_weather_accuracy_24h') }}

  -- On ne garde que les lignes réellement comparables (score calculé)

  where score_fiabilite_0_100 is not null
    and categorie_prevue is not null
),

agg as (

  select
    ville,
    categorie_prevue,

    count(*) as nb_observations,
    round(avg(score_fiabilite_0_100), 2) as score_fiabilite_moyen_0_100

  from base
  group by 1,2
)

select
  ville,
  categorie_prevue,
  nb_observations,
  score_fiabilite_moyen_0_100,

  -- Niveau de confiance "moyen" (simple et compréhensible)
  case
    when nb_observations < 8 then 'insuffisant'
    when score_fiabilite_moyen_0_100 >= 85 then 'très fiable'
    when score_fiabilite_moyen_0_100 >= 70 then 'fiable'
    when score_fiabilite_moyen_0_100 >= 55 then 'assez fiable'
    else 'pas fiable'
  end as niveau_confiance_historique

from agg
