# Weather Intelligence Platform

## Project Overview

This project builds a real-time data pipeline to analyze weather data and evaluate forecast reliability.
The goal is to detect high-risk time slots and help operational decision-making.

---

## Business Context

A European logistics company wants to anticipate the impact of weather conditions on its operations (delays, difficult conditions, instability).

The objective is to collect, structure and analyze weather data in order to create reliable risk indicators.

---

## Architecture

The project uses a modern data stack:

- Data ingestion from OpenWeatherMap API
- Real-time streaming with Apache Kafka
- Workflow orchestration with Apache Airflow
- Data storage in Google BigQuery
- Data transformation using dbt (raw -> staging -> mart)
- Dashboard and KPI visualization in Power BI

---

## Data Pipeline

1. Weather data is collected from the API every hour  
2. Events streamed through Kafka  
3. Airflow automates and manages the workflows  
4. Data is stored in BigQuery (raw layer) 
5. dbt cleans and transforms the data (staging and mart layers)  
6. KPIs calculated for risk and reliability  

---

## Data Modeling

The data model is structured in three layers:

- Raw: original API data without modification 
- Staging: cleaned and normalized data  
- Mart: analytical tables for risk and forecast analysis  

---

## Key KPIs

- Risk level per city  
- High-risk time slots detection  
- 48-hour risk forecast  
- Weather category distribution  
- Global Forecast Reliability Score  

Example insight:
Amsterdam was identified as the most unstable city during the observed period.

---

## Forecast Reliability Logic

The reliability score is calculated using: 
- Temperature difference between forecast and real observations  
- Weather category consistency  
- Risk level comparison
  
This score helps evaluate how accurate forecasts are for operational planning. 

---

## Tech Stack

**Data Engineering**
Apache Kafka, Apache Airflow, BigQuery, dbt  

**Analytics**
SQL, Power BI (DAX), Data Modeling  

**Programming**
Python (Pandas, NumPy)

---

## Skills Demonstrated

- Real-time data pipeline design  
- Workflow orchestration
- Data modeling (raw → staging → mart)
- KPI development  
- Forecast reliability analysis  
- End-to-end data project execution  

---

## Future Improvements

- Add predictive ML model  
- Improve reliability scoring  
- Add monitoring and alerting system  
