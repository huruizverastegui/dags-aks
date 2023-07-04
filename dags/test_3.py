from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import date, datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from shapely import geometry
from sqlalchemy import create_engine
from shapely.geometry.multipolygon import MultiPolygon
import re
from shapely import wkt
import geopandas as gpd
import pandas as pd
import json
import requests
import numpy as np
import xmltodict
import requests_cache
import shapely as shp
import geowrangler


## Arguments applied to the tasks, not the DAG in itself 
default_args={
    'owner':'airflow',
    'email_on_failure': False,
    'email_on_retry':False,
    'retries':2,
    'retry_delay': timedelta(minutes=1)
}

today_date=datetime.now()


# redefine GDACS api from RSS flow
def get_latest_disasters_rss():
    today_date=datetime.now()
    print(today_date)
    return today_date

with DAG(
    ## MANDATORY 
    dag_id='test_disasters',
    start_date=datetime(2022,11,28),
    default_args=default_args,
    description='sitrep disasters',
    schedule_interval='0 2 * * *',
    # no need to catch up on the previous runs
    catchup=False
) as dag:

        get_disasters_resources = PythonOperator(
            task_id="get_disasters_resources",
            python_callable=get_latest_disasters_rss
            )
    
        get_disasters_resources
