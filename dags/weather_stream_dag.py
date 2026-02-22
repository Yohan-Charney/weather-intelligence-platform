from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime, timezone,timedelta
from confluent_kafka import Producer, Consumer
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

import json
import logging


def send_to_kafka(**context):

    # configuration de kafka producer

    producer_config = {'bootstrap.servers': 'kafka.lacapsule.academy:9093' } 
    producer = Producer(producer_config)

    paris_data = context['ti'].xcom_pull(task_ids='get_paris_weather')
    rome_data = context['ti'].xcom_pull(task_ids='get_rome_weather')
    amsterdam_data = context['ti'].xcom_pull(task_ids='get_amsterdam_weather')
    lisbonne_data = context['ti'].xcom_pull(task_ids='get_lisbonne_weather')
    collected_at = datetime.now(timezone.utc).isoformat()


    data = [
     {
      'ville': 'Paris',
      "record_id": f"Paris_{datetime.fromtimestamp(paris_data['dt'], timezone.utc).isoformat()}",
      'temperature': paris_data['main']['temp'],
      'humidite': paris_data['main']['humidity'],
      'vitesse_vent' : paris_data['wind']['speed'],
      'categorie_meteo': paris_data["weather"][0]["main"],
      'description_meteo': paris_data['weather'][0]['description'],
      'time_api' : paris_data['dt'],
      'heure_run': collected_at
     },
     {
      'ville': 'Rome',
      "record_id": f"Rome_{datetime.fromtimestamp(rome_data['dt'], timezone.utc).isoformat()}",
      'temperature': rome_data['main']['temp'],
      'humidite': rome_data['main']['humidity'],
      'vitesse_vent' : rome_data['wind']['speed'],
      'categorie_meteo': rome_data["weather"][0]["main"],
      'description_meteo': rome_data['weather'][0]['description'],
      'time_api' : rome_data['dt'],
      'heure_run': collected_at
     },
     {
      'ville': 'Amsterdam',
      "record_id": f"Amsterdam_{datetime.fromtimestamp(amsterdam_data['dt'], timezone.utc).isoformat()}",
      'temperature': amsterdam_data['main']['temp'],
      'humidite': amsterdam_data['main']['humidity'],
      'vitesse_vent' : amsterdam_data['wind']['speed'],
      'categorie_meteo': amsterdam_data["weather"][0]["main"],
      'description_meteo': amsterdam_data['weather'][0]['description'],
      'time_api' : amsterdam_data['dt'],
      'heure_run': collected_at
     },
     {
      'ville': 'Lisbon',
      "record_id": f"Lisbon_{datetime.fromtimestamp(lisbonne_data['dt'], timezone.utc).isoformat()}",
      'temperature': lisbonne_data['main']['temp'],
      'humidite': lisbonne_data['main']['humidity'],
      'vitesse_vent' : lisbonne_data['wind']['speed'],
      'categorie_meteo': lisbonne_data["weather"][0]["main"],
      'description_meteo': lisbonne_data['weather'][0]['description'],
      'time_api' : lisbonne_data['dt'],
      'heure_run': collected_at
     }
    ]

    # envoi de chaque row a kafka sur un le topic ******************weather-stream
    compteur = 0
    for rec in data:
            producer.produce(
                topic = 'weather-stream',
                value = json.dumps(rec).encode("utf-8"),
                key=rec['ville'].encode("utf-8")
            )
            compteur += 1

    producer.flush() # forcer l'envoi des messages

    print(f"{compteur} Données envoyées à Kafka")


def send_to_bigquery(**context):

    # connexion a big query

    hook = BigQueryHook(
    gcp_conn_id="bigquery",
    use_legacy_sql=False
    )
    project_id = "airflow"
    dataset_id = "weather_stream_us"
    table_id = "raw_weather"
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    # Créer le dataset, table si elle n'existe pas

    create_dataset_sql = (
    f'CREATE SCHEMA IF NOT EXISTS `{project_id}.{dataset_id}` '
    f'OPTIONS(location="US")'
    )
    hook.run(create_dataset_sql)

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{full_table_id}` (
    record_id STRING,
    ville STRING,
    temperature FLOAT64,
    humidite INT64,
    vitesse_vent FLOAT64,
    categorie_meteo STRING,
    description_meteo STRING,
    time_api INT64,
    heure_run STRING
    )"""

    hook.run(create_table_sql)

    # Configuration du kafka consumer
    consumer_config = {
        'bootstrap.servers': 'kafka.lacapsule.academy:9093',
        'group.id': f'weather-stream-consumer',
        'auto.offset.reset': 'latest'
    }

    consumer = Consumer(consumer_config)
    consumer.subscribe(['weather-stream'])

    compteur = 0
    none_count = 0

    # remplissage des tables
    while True :
        message = consumer.poll(2.0)
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
            SELECT 
                @record_id AS record_id,
                @ville AS ville,
                @temperature AS temperature,
                @humidite AS humidite,
                @vitesse_vent AS vitesse_vent,
                @categorie_meteo AS categorie_meteo,
                @description_meteo AS description_meteo,
                @time_api AS time_api,
                @heure_run AS heure_run
            ) S
            ON T.record_id = S.record_id
            WHEN NOT MATCHED THEN
            INSERT (record_id, ville, temperature, humidite, vitesse_vent, categorie_meteo, description_meteo, time_api, heure_run)
            VALUES (S.record_id, S.ville, S.temperature, S.humidite, S.vitesse_vent, S.categorie_meteo, S.description_meteo, S.time_api, S.heure_run)
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("record_id", "STRING", data_cons.get("record_id")),
                    bigquery.ScalarQueryParameter("ville", "STRING", data_cons.get("ville")),
                    bigquery.ScalarQueryParameter("temperature", "FLOAT64", temperature),
                    bigquery.ScalarQueryParameter("humidite", "INT64", humidite),
                    bigquery.ScalarQueryParameter("vitesse_vent", "FLOAT64", data_cons.get("vitesse_vent")),
                    bigquery.ScalarQueryParameter("categorie_meteo", "STRING", categorie),
                    bigquery.ScalarQueryParameter("description_meteo", "STRING", description),
                    bigquery.ScalarQueryParameter("time_api", "INT64", data_cons.get("time_api")),
                    bigquery.ScalarQueryParameter("heure_run", "STRING", data_cons.get("heure_run")),
                ]
            )

            client.query(query, job_config=job_config).result()
            compteur += 1

        except Exception as e:
            logging.exception("Erreur insertion BigQuery (stacktrace complète)")
            continue
    if compteur == 0:
        logging.info(f"Aucun nouveau message à consommer depuis Kafka.")
    else:
        logging.info(f"Chargement terminé. Total Ligne insérée dans BigQuery : {compteur}")

    consumer.close()


with DAG(
    dag_id="eather_stream_dag",
    start_date=datetime(2025,1,1),
    schedule_interval="@hourly",
    catchup=False
) as dag:
      
    get_paris_weather = SimpleHttpOperator(
        task_id='get_paris_weather',
        method='GET',
        http_conn_id='openweathermap_api',  # Connexion HTTP à configurer dans Airflow avec la clé API
        endpoint='data/2.5/weather',
        data={"q": "Paris,FR","units": "metric"},  # Remplacer YOUR_API_KEY par votre clé API
        response_filter=lambda response: response.json(),  # Pour traiter la réponse en JSON
        log_response=True,
        retries=2,
        retry_delay=timedelta(minutes = 2),
    )

    get_rome_weather = SimpleHttpOperator(
        task_id='get_rome_weather',
        method='GET',
        http_conn_id='openweathermap_api',  # Connexion HTTP à configurer dans Airflow avec la clé API
        endpoint='data/2.5/weather',
        data={"q": "Rome,IT","units": "metric"},  # Remplacer YOUR_API_KEY par votre clé API
        response_filter=lambda response: response.json(),  # Pour traiter la réponse en JSON
        log_response=True,
        retries=2,
        retry_delay=timedelta(minutes = 2),
    )

    get_amsterdam_weather = SimpleHttpOperator(
        task_id='get_amsterdam_weather',
        method='GET',
        http_conn_id='openweathermap_api',  # Connexion HTTP à configurer dans Airflow avec la clé API
        endpoint='data/2.5/weather',
        data={"q": "Amsterdam,NL","units": "metric"},  # Remplacer YOUR_API_KEY par votre clé API
        response_filter=lambda response: response.json(),  # Pour traiter la réponse en JSON
        log_response=True,
        retries=2,
        retry_delay=timedelta(minutes = 2),
    )

    get_lisbonne_weather = SimpleHttpOperator(
        task_id = 'get_lisbonne_weather',
        method = 'GET',
        http_conn_id = 'openweathermap_api',  # Connexion HTTP à configurer dans Airflow avec la clé API
        endpoint = 'data/2.5/weather',
        data = {"q": "Lisbon,PT","units": "metric"},  # Remplacer YOUR_API_KEY par votre clé API
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
   

    dbt_run = DbtCloudRunJobOperator(
        task_id = "dbt_run",
        dbt_cloud_conn_id = "dbt_default",
        account_id = 123456789,
        job_id = 1234567890,
        wait_for_termination=True, 
        check_interval=60,  
        timeout=3600,

    )


    [get_paris_weather, get_rome_weather, get_amsterdam_weather, get_lisbonne_weather] >> kafka  >>  bigquery_task >> dbt_run
