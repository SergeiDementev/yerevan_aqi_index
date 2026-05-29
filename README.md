# ETL pipeline based on API request, Airflow and PostgreSQL. 

The idea of the project is to collect data from the sensor to count Air Quality Index and analyze whether AQI provided by Purpleair is counted correctly. 

As the index counted depends not only on the PM2.5 (microscopic airborne particles measuring 2.5 micrometers or less in diameter), but also  humidity and temperature. 

Collected data:
- date / time (timestamp);
- humidity;
- temperature;
- pm2_5.

Pipeline connects purpleair.com, gets data from a chosen sensor, and inserts data into local PostgreSQL database every 5 minutes. 

The API token for connection purpleair.com taken from https://develop.purpleair.com/ (registration is free).

This DAG uses Airflow Connection via PosgresHook and internal Variables to hide my personal API key (should be configured via the Airflow interface).

Two minor data transformations performed in the script: 

- temperature is transformed from Fahrenheit into Celsius and rounded to 1 digit after the decimal point (just as its easier to read data and no extreme accuracy needed);
- the original date_time information provided by the sensor (timestamp UTC +0) transformed by my PostgreSQL server using my local timezone.  

Though I understand that it's more correct to keep the raw source data untouched and do all the transformation separately, it's just a simple study project and small compromises are acceptable. 

Airflow v2.10.2 
PostgreSQL 
Python 3.10
