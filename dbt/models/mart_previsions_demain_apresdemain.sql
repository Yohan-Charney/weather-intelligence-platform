{{ config(materialized='table') }}

-- Table finale "opérations" :
-- Prévisions de demain & après-demain (UTC) par ville et créneau,
-- avec :
--  - risque logistique prévu (0/1/2 + libellé)
--  - confiance historique moyenne par ville + catégorie prévue (score + niveau)
-- Objectif : si demain il pleut, dire si historiquement "pluie" est fiable dans cette ville.

with forecast_base as (

  select
    ville,
    date_heure_prevue,
    date_heure_collecte,
    horizon_heures,

    temperature_prevue,
    vent_prevu,
    humidite_prevue,
    categorie_prevue,
    description_prevue

  from {{ ref('stg_forecast') }}
  where date(date_heure_prevue) between date_add(current_date('UTC'), interval 1 day)
                                 and date_add(current_date('UTC'), interval 2 day)

),

forecast_dedup as (

  -- On garde la dernière collecte pour chaque ville + créneau prévu
  select
    *,
    row_number() over (
      partition by ville, date_heure_prevue
      order by date_heure_collecte desc
    ) as rn
  from forecast_base

),

forecast_scored as (

  select
    ville,
    date_heure_prevue,
    date(date_heure_prevue) as jour_prevu,
    extract(hour from date_heure_prevue) as heure_prevue,

    date_heure_collecte as date_heure_collecte_utilisee,
    horizon_heures,

    round(temperature_prevue, 2) as temperature_prevue,
    round(vent_prevu, 2) as vent_prevu,
    round(humidite_prevue, 2) as humidite_prevue,

    categorie_prevue,
    description_prevue,

    -- Risque basé sur la PREVISION
    case
      when temperature_prevue < -10 then 2
      when temperature_prevue > 45 then 2
      when categorie_prevue = 'Snow' then 2
      when categorie_prevue = 'Thunderstorm' then 2
      when vent_prevu >= 20 then 2

      when temperature_prevue between 35 and 45 then 1
      when temperature_prevue >= -10 and temperature_prevue < -5 then 1
      when categorie_prevue in ('Rain') then 1
      when vent_prevu >= 17 then 1

      else 0
    end as indice_risque_meteo_prevu

  from forecast_dedup
  where rn = 1

),

ref_fiabilite as (

  -- Référence historique : ville + catégorie prévue => score moyen + niveau
  select
    ville,
    categorie_prevue as categorie_prevue_ref,  -- attendu en anglais (Rain, Clouds, ...)
    nb_observations,
    score_fiabilite_moyen_0_100,
    niveau_confiance_historique
  from {{ ref('mart_fiabilite_reference_ville_categorie_24h') }}

)

select
  f.ville,
  f.date_heure_prevue,
  f.jour_prevu,
  f.heure_prevue,

  f.date_heure_collecte_utilisee,
  f.horizon_heures,

  f.temperature_prevue,
  f.vent_prevu,
  f.humidite_prevue,

  -- Catégorie prévue FR (affichage jury)
  case
    when f.categorie_prevue = 'Rain' then 'Pluie'
    when f.categorie_prevue = 'Clouds' then 'Nuageux'
    when f.categorie_prevue = 'Clear' then 'Ciel dégagé'
    when f.categorie_prevue = 'Snow' then 'Neige'
    when f.categorie_prevue = 'Thunderstorm' then 'Orage'
    when f.categorie_prevue = 'Drizzle' then 'Bruine'
    when f.categorie_prevue is null then 'Inconnu'
    else f.categorie_prevue
  end as categorie_prevue_fr,

  f.categorie_prevue as categorie_prevue_en,
  f.description_prevue,

  -- Risque prévu
  f.indice_risque_meteo_prevu,
  case
    when f.indice_risque_meteo_prevu = 0 then 'Aucun risque'
    when f.indice_risque_meteo_prevu = 1 then 'Risque modéré'
    else 'Risque fort'
  end as niveau_risque_prevu,

  -- Confiance historique associée à la catégorie prévue (ville + catégorie)
  r.nb_observations,
  r.score_fiabilite_moyen_0_100,
  r.niveau_confiance_historique,

  -- Recommandation lisible
  case
    when r.nb_observations is null then 'Confiance inconnue (pas d’historique)'
    when r.nb_observations < 10 then 'Données insuffisantes : prudence'
    when r.niveau_confiance_historique in ('très fiable','fiable') then 'Prévision à suivre'
    when r.niveau_confiance_historique = 'assez fiable' then 'Prévision plausible'
    else 'Prévision incertaine : prévoir une marge'
  end as recommandation

from forecast_scored f
left join ref_fiabilite r
  on r.ville = f.ville
 and r.categorie_prevue_ref = f.categorie_prevue
