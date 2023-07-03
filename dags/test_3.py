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
#import h3
import requests_cache
import shapely as shp
import geowrangler
from geowrangler import grids

#Define the hex size
hex_granularity=8

#define the connection id to postres
POSTGRES_CONN_ID="postgres_datafordecision"

## Arguments applied to the tasks, not the DAG in itself 
default_args={
    'owner':'airflow',
    'email_on_failure': False,
    'email_on_retry':False,
    'retries':2,
    'retry_delay': timedelta(minutes=1)
}

today_date=datetime.now()


