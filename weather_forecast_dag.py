    
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime, timezone,timedelta
from confluent_kafka import Producer, Consumer
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
import json
import logging
import uuid


def send_to_kafka(**context):
    """
    Objectif :
    Récupérer les prévisions météo OpenWeather (5 jours / pas de 3h),
    et envoyer CHAQUE créneau prévu vers Kafka.

    L'API retourne une LISTE de créneaux (environ 40).
    On doit donc itérer sur chaque élément de la liste.
    """

    # Configuration du producteur Kafka
    producteur = Producer({
        'bootstrap.servers': 'kafka.lacapsule.academy:9093'
    })

    # Timestamp de collecte (moment où le DAG tourne)
    date_collecte = datetime.now(timezone.utc).isoformat()

    # Récupération des réponses API depuis XCom
    previsions_par_ville = {
        "Paris": context['ti'].xcom_pull(task_ids='get_paris_forecast'),
        "Rome": context['ti'].xcom_pull(task_ids='get_rome_forecast'),
        "Amsterdam": context['ti'].xcom_pull(task_ids='get_amsterdam_forecast'),
        "Lisbon": context['ti'].xcom_pull(task_ids='get_lisbon_forecast'),
    }

    compteur = 0

    # Boucle sur chaque ville
    for ville, reponse_api in previsions_par_ville.items():

        # La clé "list" contient tous les créneaux forecast (3h, 6h, 9h, ...)
        liste_previsions = reponse_api.get("list", [])

        # Boucle sur chaque créneau prévu
        for element in liste_previsions:

            message = {
                "ville": ville,

                # Identifiant unique : Ville + Timestamp prévu
                "record_forecast_id": f"{ville}_{datetime.fromtimestamp(element['dt'], timezone.utc).isoformat()}",

                "temperature": element["main"]["temp"],
                "humidite": element["main"]["humidity"],
                "vitesse_vent": element["wind"]["speed"],

                "categorie_meteo": element["weather"][0]["main"],
                "description_meteo": element["weather"][0]["description"],

                # Timestamp prévu (UNIX)
                "time_api": element["dt"],
                "forecast_timestamp": element["dt"],

                # Timestamp de collecte du pipeline
                "heure_run": date_collecte
            }

            # Envoi vers Kafka
            producteur.produce(
                topic='***********forecast',
                value=json.dumps(message).encode("utf-8"),
                key=ville.encode("utf-8")
            )

            compteur += 1

    # Forcer l'envoi des messages
    producteur.flush()

    print(f"{compteur} créneaux de prévision envoyés à Kafka")




def send_to_bigquery(**context):

    # connexion a big query

    hook = BigQueryHook(
    gcp_conn_id="**********bigquery",
    use_legacy_sql=False
    )
    project_id = "airflow-486914"
    dataset_id = "weather_stream_us"
    table_id = "raw_forecast"
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    # Créer le dataset, table si elle n'existe pas

    create_dataset_sql = (
    f'CREATE SCHEMA IF NOT EXISTS `{project_id}.{dataset_id}` '
    f'OPTIONS(location="US")'
    )
    hook.run(create_dataset_sql)

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{full_table_id}` (
    record_forecast_id STRING,
    ville STRING,
    temperature FLOAT64,
    humidite INT64,
    vitesse_vent FLOAT64,
    categorie_meteo STRING,
    description_meteo STRING,
    time_api INT64,
    heure_run STRING,
    forecast_timestamp INT64
    )"""

    hook.run(create_table_sql)
    add_column_sql = f"""
    ALTER TABLE `airflow-486914.weather_stream.raw_forecast`
    ADD COLUMN IF NOT EXISTS forecast_timestamp INT64 """
    hook.run(add_column_sql)

    
    # Configuration du kafka consumer
    consumer_config = {
        'bootstrap.servers': 'kafka.lacapsule.academy:9093',
        'group.id': '**************weather-forecast-consumer-v',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(consumer_config)
    consumer.subscribe(['**************forecast'])

    compteur = 0
    none_count = 0

    # remplissage des tables
    while True :
        message = consumer.poll(3.0)
        if message is None: 
           none_count +=1
           if none_count >= 10:
               break 
           else:
               continue
        else:
            none_count = 0
        if message.error(): 
           logging.error(f"Erreur Kafka : {message.error()}") 
           continue
        try:
            data_cons = json.loads(message.value().decode("utf-8"))
        except Exception:
            logging.exception(f"Erreur décodage JSON")
            continue

        try:

            # Préparation des lignes pour BigQuery (compat anciens noms avec accents)
            client = hook.get_client(project_id=project_id)

            # mapping (compat accents)
            temperature = data_cons.get("temperature", data_cons.get("température"))
            humidite = data_cons.get("humidite", data_cons.get("humidité"))
            categorie = data_cons.get("categorie_meteo", data_cons.get("catégorie météo"))
            description = data_cons.get("description_meteo", data_cons.get("description météo"))

            query = f"""
                MERGE `{full_table_id}` T
                USING (
                (SELECT
                    @record_forecast_id AS record_forecast_id,
                    @ville AS ville,
                    @temperature AS temperature,
                    @humidite AS humidite,
                    @vitesse_vent AS vitesse_vent,
                    @categorie_meteo AS categorie_meteo,
                    @description_meteo AS description_meteo,
                    @time_api AS time_api,
                    @heure_run AS heure_run,
                    @forecast_timestamp AS forecast_timestamp
                )
                ) S
                ON T.record_forecast_id = S.record_forecast_id
                WHEN NOT MATCHED THEN
                INSERT (record_forecast_id, ville, temperature, humidite, vitesse_vent, categorie_meteo, description_meteo, time_api, heure_run, forecast_timestamp)
                VALUES (S.record_forecast_id, S.ville, S.temperature, S.humidite, S.vitesse_vent, S.categorie_meteo, S.description_meteo, S.time_api, S.heure_run, S.forecast_timestamp)
                """



            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("record_forecast_id", "STRING", data_cons.get("record_forecast_id")),
                    bigquery.ScalarQueryParameter("ville", "STRING", data_cons.get("ville")),
                    bigquery.ScalarQueryParameter("temperature", "FLOAT64", temperature),
                    bigquery.ScalarQueryParameter("humidite", "INT64", humidite),
                    bigquery.ScalarQueryParameter("vitesse_vent", "FLOAT64", data_cons.get("vitesse_vent")),
                    bigquery.ScalarQueryParameter("categorie_meteo", "STRING", categorie),
                    bigquery.ScalarQueryParameter("description_meteo", "STRING", description),
                    bigquery.ScalarQueryParameter("time_api", "INT64", data_cons.get("time_api")),
                    bigquery.ScalarQueryParameter("heure_run", "STRING", data_cons.get("heure_run")),
                    bigquery.ScalarQueryParameter("forecast_timestamp", "INT64", data_cons.get("forecast_timestamp")),
                ]
            )

            client.query(query, job_config=job_config).result()
            compteur += 1
            logging.info(f"Ligne insérée dans BigQuery {compteur}")

        except Exception as e:
            logging.exception("Erreur insertion BigQuery (stacktrace complète)")
            continue
    if compteur == 0:
        logging.info(f"Aucun nouveau message à consommer depuis Kafka.")
    else:
        logging.info(f"Chargement terminé. Total inséré : {compteur}")

    consumer.close()


with DAG(
    dag_id="user_yoan_charney_mboulou_weather_forecast",
    start_date=datetime(2025,1,1),
    schedule_interval="0 */6 * * *",
    catchup=False,
    max_active_runs=1

) as dag:
      
    get_paris_forecast = SimpleHttpOperator(
        task_id='get_paris_forecast',
        method='GET',
        http_conn_id='**************openweathermap_api',  # Connexion HTTP à configurer dans Airflow avec la clé API
        endpoint='data/2.5/forecast',
        data={"q": "Paris,FR", "appid": "****************","units": "metric"},  # Remplacer YOUR_API_KEY par votre clé API
        response_filter=lambda response: response.json(),  # Pour traiter la réponse en JSON
        log_response=True,
        retries=2,
        retry_delay=timedelta(minutes = 2),
    )

    get_rome_forecast = SimpleHttpOperator(
        task_id='get_rome_forecast',
        method='GET',
        http_conn_id='************openweathermap_api',  # Connexion HTTP à configurer dans Airflow avec la clé API
        endpoint='data/2.5/forecast',
        data={"q": "Rome,IT", "appid": "***********************","units": "metric"},  # Remplacer YOUR_API_KEY par votre clé API
        response_filter=lambda response: response.json(),  # Pour traiter la réponse en JSON
        log_response=True,
        retries=2,
        retry_delay=timedelta(minutes = 2),
    )

    get_amsterdam_forecast = SimpleHttpOperator(
        task_id='get_amsterdam_forecast',
        method='GET',
        http_conn_id='********openweathermap_api',  # Connexion HTTP à configurer dans Airflow avec la clé API
        endpoint='data/2.5/forecast',
        data={"q": "Amsterdam,NL", "appid": "***********************","units": "metric"},  # Remplacer YOUR_API_KEY par votre clé API
        response_filter=lambda response: response.json(),  # Pour traiter la réponse en JSON
        log_response=True,
        retries=2,
        retry_delay=timedelta(minutes = 2),
    )

    get_lisbonne_forecast = SimpleHttpOperator(
        task_id = 'get_lisbon_forecast',
        method = 'GET',
        http_conn_id = '***********openweathermap_api',  # Connexion HTTP à configurer dans Airflow avec la clé API
        endpoint = 'data/2.5/forecast',
        data = {"q": "Lisbon,PT", "appid": "***************************","units": "metric"},  # Remplacer YOUR_API_KEY par votre clé API
        response_filter = lambda response: response.json(),  # Pour traiter la réponse en JSON
        log_response = True,
        retries = 2,
        retry_delay = timedelta(minutes = 2),
    )

    kafka = PythonOperator(
        task_id = "send_to_kafka",
        python_callable = send_to_kafka,
    )

    bigquery_task = PythonOperator(
        task_id = "send_to_bigquery",
        python_callable = send_to_bigquery,
    )


    [get_paris_forecast, get_rome_forecast, get_amsterdam_forecast, get_lisbonne_forecast] >> kafka  >>  bigquery_task