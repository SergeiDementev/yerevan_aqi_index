# ETL pipeline based on API request, Airflow and PostgreSQL. 

Pipeline connects purpleair.com, gets data from a chosen sensor, and inserts data into local PostgreSQL database every 5 minutes. 

The API token for connection purpleair.com taken from https://develop.purpleair.com/ 

This DUG uses Airflow Connection via PosgresHook and internal Variables to hide my personal API key

Airflow v2.10.2 (https://pypi.python.org/pypi/apache-airflow/2.10.2)
PostgreSQL 
Python 3.10
