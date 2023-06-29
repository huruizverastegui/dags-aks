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

#country list for eapro region - to filter 

country_list_eapr= ["Australia",
                   "Brunei",
                   "Cambodia",
                   "Cook",
                   "Fiji",
                   "Indonesia",
                   "China",
                   "Japan",
                   "Korea",
                   "Kiribati",
                   "Laos",
                   "Malaysia",
                   "Marshall",
                   "Micronesia",
                   "Mongolia",
                   "Myanmar",
                   "Nauru",
                   "Zealand",
                   "Niue",
                   "Palau",
                   "Papua",
                   "Philippines",
                   "Samoa",
                   "Singapore",
                   "Solomon",
                   "Thailand",
                   "Taiwan",
                   "Timor",
                   "Tonga",
                   "Tuvalu",
                   "Vanuatu",
                   "Vietnam"] 


regexp_eapr_str= ('|').join(country_list_eapr)

def is_in_eapr(disaster_name):
    if disaster_name is None:
        return 1 
    elif re.search(regexp_eapr_str.lower().replace(" ", ""), disaster_name.lower().replace(" ", "")):
        return 1
    else :
        return 0

def get_bbox(row):
    if row == None:
        results=None 
    else:
        results= shp.geometry.box(*row.bounds, ccw=True)
    return results

# transform the list of coordinates into a polygon in the case we extract the info from the rss flux
def get_polygon_rss(geojson_url):
    try:
        print(geojson_url)
        poly_list=xmltodict.parse(requests.get(geojson_url).content)['alert']['info']['area']
        if type(poly_list) is dict:
            inter= poly_list['polygon'].split(" ")
            pointList=([geometry.Point(np.float_(re.sub(r"\)|\(", "",p).split(",") if p.count(",")>=1 else re.sub(r"\)|\(", "",p))) for p in inter])
            #x and y are reversed because lat and lon are reverse in the rss flux
            result = geometry.Polygon([[p.y, p.x] for p in pointList])

        else:
            inter=[element['polygon'].split(" ") for element in poly_list]
            list_poly=[]
            for p in inter: 
                pointList=([geometry.Point(np.float_(point.split(","))) for point in p])
                #x and y are reversed because lat and lon are reverse in the rss flux
                poly = geometry.Polygon([[p.y, p.x] for p in pointList])
                list_poly.append(poly)
            result=MultiPolygon(list_poly)
        return result
    except:
        return

def get_html_description(geo_url):
    json_geo=requests.get(geo_url).json()
    html_description=json_geo['features'][0]['properties']['htmldescription']
    return html_description


# function used to get all the severely impacted geometries from a disaster (Orange or Red zones)

def get_orange_geometries(geo_url,event_type):
    json_geo=requests.get(geo_url).json()
    feature_limited_geo=[]

    # for cyclons TC - get Poly Orange 
    if event_type=='TC':
        # check through all json features
        for feature in json_geo['features']:
            try:
                #if one of them has a Poly_orange class we extract the geometry
                # if no Poly Orange - we take the Poly green
                # the Poly green is given before the Poly Orange - if there is no Poly Orange the Poly green will be kept
                if feature['properties']['Class']in ['Poly_Green','Poly_Orange']:
                    # Case if it's a simple Poly
                    if feature['geometry']['type']=='Polygon':
                        feature_limited_geo=geometry.Polygon(feature['geometry']['coordinates'][0])
                    #Case if it's a multi Poly
                    elif feature['geometry']['type']=='MultiPolygon':
                        multi_pol_geo=feature['geometry']['coordinates']
                        list_poly=[]
                        for pol in multi_pol_geo:
                            poly = geometry.Polygon(pol[0])
                            list_poly.append(poly)
                        feature_limited_geo=MultiPolygon(list_poly)
            except:
                pass
    # for EQ - get Poly intensity 5.0
    if event_type=='EQ':
        # check through all json features
        for feature in json_geo['features']:
            try:
                #if one of them has a a Poly with intensity = 5.0 we get the polygon
                # sometimes there is no Polygon with intensity 5.0 because the intensity is too high, in this case we take 5.5 or 6
                if feature['properties']['intensity']in [5.0,5.5,6.0]:
                    # Case if it's a simple Poly
                    if feature['geometry']['type']=='Polygon':
                        feature_limited_geo=geometry.Polygon(feature['geometry']['coordinates'][0])
                    #Case if it's a multi Poly
                    elif feature['geometry']['type']=='MultiPolygon':
                        multi_pol_geo=feature['geometry']['coordinates']
                        list_poly=[]
                        for pol in multi_pol_geo:
                            poly = geometry.Polygon(pol[0])
                            list_poly.append(poly)
                        feature_limited_geo=MultiPolygon(list_poly)
                    #if we have found one poly of intensity 5 /5.5 /6 we stop and go to the next disaster
                    # intensity are ordered from the smallest to the biggest - so we should encounter the 5.0 first
                    break
            except:
                pass
    
     # for volcano take the Poly Circle feature - as it's the only one we have 
    if event_type=='VO':
    # check through all json features
        for feature in json_geo['features']:
            try:
                #if one of them has a label with km
                if feature['properties']['Class']=='Poly_Circle':
                    # Case if it's a simple Poly
                    if feature['geometry']['type']=='Polygon':
                        feature_limited_geo=geometry.Polygon(feature['geometry']['coordinates'][0])
                    #Case if it's a multi Poly
                    elif feature['geometry']['type']=='MultiPolygon':
                        multi_pol_geo=feature['geometry']['coordinates']
                        list_poly=[]
                        for pol in multi_pol_geo:
                            poly = geometry.Polygon(pol[0])
                            list_poly.append(poly)
                        feature_limited_geo=MultiPolygon(list_poly)
            except:
                pass
            
    if event_type=='FL':
    # check through all json features
        for feature in json_geo['features']:
            try:
                #Affected area and global area are actually reversed - the smaller one is the global area
                if feature['properties']['polygonlabel']=='Affected area':
                    # Case if it's a simple Poly
                    if feature['geometry']['type']=='Polygon':
                        feature_limited_geo=geometry.Polygon(feature['geometry']['coordinates'][0])
                    #Case if it's a multi Poly
                    elif feature['geometry']['type']=='MultiPolygon':
                        multi_pol_geo=feature['geometry']['coordinates']
                        list_poly=[]
                        for pol in multi_pol_geo:
                            poly = geometry.Polygon(pol[0])
                            list_poly.append(poly)
                        feature_limited_geo=MultiPolygon(list_poly)
            except:
                pass

    if feature_limited_geo==[]:
        feature_limited_geo=None 
    return feature_limited_geo


# function used to get all the low impacted geometries from a disaster (Green Zones) - to be used as a back up if there is no geometry in the rss get_polygon_rss

def get_global_geometries(geo_url,event_type):
    json_geo=requests.get(geo_url).json()
    feature_limited_geo=[]

    # for cyclons TC - get Poly Green 
    if event_type=='TC':
        # check through all json features
        for feature in json_geo['features']:
            try:
                #if one of them has a Poly_orange class we extract the geometry
                if feature['properties']['Class']=='Poly_Green':
                    # Case if it's a simple Poly
                    if feature['geometry']['type']=='Polygon':
                        feature_limited_geo=geometry.Polygon(feature['geometry']['coordinates'][0])
                    #Case if it's a multi Poly
                    elif feature['geometry']['type']=='MultiPolygon':
                        multi_pol_geo=feature['geometry']['coordinates']
                        list_poly=[]
                        for pol in multi_pol_geo:
                            poly = geometry.Polygon(pol[0])
                            list_poly.append(poly)
                        feature_limited_geo=MultiPolygon(list_poly)
            except:
                pass
            
    # for  EQ - take the poly circle around the epicenter

    if event_type=='EQ':
        # check through all json features
        for feature in json_geo['features']:
            try:
                if feature['properties']['Class']=='Poly_Circle':
                    # Case if it's a simple Poly
                    if feature['geometry']['type']=='Polygon':
                        feature_limited_geo=geometry.Polygon(feature['geometry']['coordinates'][0])
                    #Case if it's a multi Poly
                    elif feature['geometry']['type']=='MultiPolygon':
                        multi_pol_geo=feature['geometry']['coordinates']
                        list_poly=[]
                        for pol in multi_pol_geo:
                            poly = geometry.Polygon(pol[0])
                            list_poly.append(poly)
                        feature_limited_geo=MultiPolygon(list_poly)
                    
            except:
                pass


    # for volcano take the Poly Circle feature - as it's the only one we have 

    if event_type=='VO':
    # check through all json features
        for feature in json_geo['features']:
            try:
                #if one of them has a label with km
                if feature['properties']['Class']=='Poly_Circle':
                    # Case if it's a simple Poly
                    if feature['geometry']['type']=='Polygon':
                        feature_limited_geo=geometry.Polygon(feature['geometry']['coordinates'][0])
                    #Case if it's a multi Poly
                    elif feature['geometry']['type']=='MultiPolygon':
                        multi_pol_geo=feature['geometry']['coordinates']
                        list_poly=[]
                        for pol in multi_pol_geo:
                            poly = geometry.Polygon(pol[0])
                            list_poly.append(poly)
                        feature_limited_geo=MultiPolygon(list_poly)
            except:
                pass
        
    if event_type=='FL':
    # check through all json features
        for feature in json_geo['features']:
            try:
                #Global and Affected areas are reversed - Affected area is the biggest area for Floods
                if feature['properties']['polygonlabel']=='Global area':
                    # Case if it's a simple Poly
                    if feature['geometry']['type']=='Polygon':
                        feature_limited_geo=geometry.Polygon(feature['geometry']['coordinates'][0])
                    #Case if it's a multi Poly
                    elif feature['geometry']['type']=='MultiPolygon':
                        multi_pol_geo=feature['geometry']['coordinates']
                        list_poly=[]
                        for pol in multi_pol_geo:
                            poly = geometry.Polygon(pol[0])
                            list_poly.append(poly)
                        feature_limited_geo=MultiPolygon(list_poly)
            except:
                pass

    if feature_limited_geo==[]:
        feature_limited_geo=None 
    return feature_limited_geo

# function to get the h3

def get_h3(row):
    if row==[] or row is None :
        h3_list=[]
    else:
    #INITIATE h3 generator
        h3_generator=grids.H3GridGenerator(resolution=hex_granularity,return_geometry=False)
        df_row=pd.DataFrame()
        df_row['geometry']=[row]
        df_row = gpd.GeoDataFrame(df_row)
        df_row=df_row.set_crs('epsg:4326')
        h3_grid=h3_generator.generate_grid(df_row)
        h3_list=h3_grid.values.tolist()
        h3_list=[item for sublist in h3_list for item in sublist]
    return h3_list

# function to get the event id in the case we extract the info from the gdac-api library
def make_event_id_from_url(url):
    return url.split('https://www.gdacs.org/report.aspx?eventid=')[1].split('&')[0]


# redefine GDACS api from RSS flow
def get_latest_disasters_rss():
    res = requests.get("https://www.gdacs.org/xml/rss.xml")
    xml_parser = xmltodict.parse(res.content)
    events = [item  for item in xml_parser["rss"]["channel"]["item"]]

    #transform to pd dataframe
    eventsframe = pd.DataFrame(events)
    
    # set alertscore as float
    eventsframe['gdacs:alertscore'] = eventsframe['gdacs:alertscore'].astype(float)

    #keep only events where alertscore >=1 and EQ/TC/FL/VO 
    important_events = eventsframe[eventsframe['gdacs:alertscore']>=0.9]
   # important_events = eventsframe[eventsframe['gdacs:eventtype']=='VO']
    important_events = important_events[important_events['gdacs:eventtype'].isin(['EQ','TC','FL','VO'])]

   # keep only the disasters in the region 

    important_events['is_in_eapr']=important_events['gdacs:country'].apply(is_in_eapr)

    important_events=important_events[important_events['is_in_eapr']==1]

    # keep only important columns
    summary=important_events[['gdacs:eventid',
                              'gdacs:fromdate',
                              'gdacs:todate',
                              'gdacs:iscurrent',
                              'gdacs:eventtype',
                              'gdacs:alertscore',
                              'title',
                              'gdacs:country',
                              'gdacs:cap',
                              'link',
                              'gdacs:bbox'
                                  ]]


    # geo url that will be used to get the orange polygons for the disasters
    summary["geo_url"]="https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype="+summary["gdacs:eventtype"]+"&eventid="+summary["gdacs:eventid"]

    #tranform the bbox string into a proper list of float                           
    #summary['gdacs:bbox'] = summary['gdacs:bbox'].apply(get_bbox_format_rss)

    #transform the list into a shapely geometry
    #summary['gdacs:bbox'] = summary['gdacs:bbox'].apply(make_shapely_bbox)

    #Open the url , extract the list of points as a polygon and transform it into a shapely geometry
    #summary['geometry_1'] = summary['gdacs:cap'].apply(get_polygon_rss)

    #if geometry cannot be extracted from the gdacs:cap - use the get_global_geometries function
    summary['geometry'] = summary.apply(lambda x: get_global_geometries(x['geo_url'], x['gdacs:eventtype']), axis=1)    

    # coalesce geometry 1 and 2 + delete the former columns
    #summary['geometry']=summary.geometry_1.combine_first(summary.geometry_2)
    #summary=summary.drop(columns=['geometry_1','geometry_2'])

    #Open the url , extract the list of points as a polygon and transform it into a shapely geometry
    summary['geometry_validated'] = summary.apply(lambda x: get_orange_geometries(x['geo_url'], x['gdacs:eventtype']), axis=1)

    
    
    # get the bbox based on the geometry 
    summary = gpd.GeoDataFrame(summary)
    summary.set_crs('epsg:4326')
    summary['gdacs:bbox']=summary['geometry'].apply(get_bbox)
    
    #add DB update date
    summary['update_date'] =today_date

    #get the html description from the geourl 
    summary['htmldescription'] = summary['geo_url'].apply(get_html_description)

    #cchange event id to int
    summary['gdacs:eventid']=summary['gdacs:eventid'].astype(int)

    # get the h3 list 
    summary['h3_list'] = summary['geometry_validated'].apply(get_h3)

    # create another dataframe holding only the h3 list and eventid
    df_hex = summary[['gdacs:eventid','gdacs:eventtype','h3_list','update_date']]

    # explode the h3 list into a new row for each h3
    df_hex = df_hex.explode('h3_list')


    #drop the h3_list column in summary df
    summary.drop(columns=['h3_list'], inplace=True)    

    summary.to_csv('/tmp/latest_disasters.csv', index=False)
    df_hex.to_csv('/tmp/latest_disasters_hex.csv', index=False)

    return 


def pg_extract_disasters(copy_sql):
  pg_hook = PostgresHook.get_hook(POSTGRES_CONN_ID)
  pg_hook.copy_expert(copy_sql, '/tmp/latest_disasters.csv')

def pg_extract_hex(copy_sql):
  pg_hook = PostgresHook.get_hook(POSTGRES_CONN_ID)
  pg_hook.copy_expert(copy_sql, '/tmp/latest_disasters_hex.csv')


with DAG(
    ## MANDATORY 
    dag_id='sitrep_disasters',
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

        create_disasters_table = PostgresOperator(
            task_id="create_disasters_table",
            postgres_conn_id="postgres_datafordecision",
            sql="sql_scripts/sitrep_disasters_rss.sql"
        )


        fill_disasters_table = PythonOperator(
            task_id="fill_disasters_table",
            python_callable=pg_extract_disasters,
            op_kwargs={
                "copy_sql": "COPY disasters FROM STDIN WITH CSV HEADER DELIMITER as ','"
                }
        )

        disasters_deduplicate = PostgresOperator(
            task_id="remove_duplicates",
            postgres_conn_id="postgres_datafordecision",
            sql="sql_scripts/sitrep_disasters_rss_duplicates.sql"
        )

       
        create_hex_table = PostgresOperator(
            task_id="create_hex_table",
            postgres_conn_id="postgres_datafordecision",
            sql="sql_scripts/sitrep_hex_rss.sql"
        )


        fill_hex_table = PythonOperator(
            task_id="fill_hex_table",
            python_callable=pg_extract_hex,
            op_kwargs={
                "copy_sql": "COPY disasters_hex_inter FROM STDIN WITH CSV HEADER DELIMITER as ','"
                }
        )

        collate_hex_table = PostgresOperator(
            task_id="collate_hex_table",
            postgres_conn_id="postgres_datafordecision",
            sql="sql_scripts/sitrep_hex_rss_collate.sql"
        )

        hex_deduplicate = PostgresOperator(
            task_id="remove_hex_duplicates",
            postgres_conn_id="postgres_datafordecision",
            sql="sql_scripts/sitrep_hex_rss_duplicates.sql"
        )
         
        create_population_region_table = PostgresOperator(
            task_id="create_population_region_table",
            postgres_conn_id="postgres_datafordecision",
            sql="sql_scripts/population_region.sql"
        )


        get_disasters_resources>>create_disasters_table>>fill_disasters_table>>disasters_deduplicate
        get_disasters_resources>>create_hex_table>>fill_hex_table>>collate_hex_table>>hex_deduplicate
