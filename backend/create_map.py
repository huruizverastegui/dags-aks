from base64 import b64encode
import pickle
import random
import geopandas as gpd
import plotly.graph_objects as go
import pandas as pd
from geojson import Feature, Point, FeatureCollection, Polygon
import json
import h3pandas
import h3
import plotly.express as px
from pathlib import Path

current_dir = Path(__file__).parent.absolute()


def generate_map(event_id, engine):

    sql_query = f"SELECT event_id,country,name,geometry_validated,bbox from disasters WHERE event_id = {event_id}"
    disasters = gpd.read_postgis(sql_query, engine, geom_col="geometry_validated")
    disasters["centroid"] = disasters["geometry_validated"][0].centroid
    disasters["lat"] = disasters["centroid"][0].y
    disasters["lon"] = disasters["centroid"][0].x

    fig = go.Figure(
        go.Scattermapbox(
            lat=disasters["lat"],
            lon=disasters["lon"],
            mode="markers",
            marker=go.scattermapbox.Marker(
                size=10,
                color="rgb(242, 106, 33)",
            ),
        )
    )

    fig.add_trace(
        go.Scattermapbox(
            lat=disasters["lat"],
            lon=disasters["lon"],
            mode="markers",
            marker=go.scattermapbox.Marker(
                size=100, color="rgb(242, 106, 33)", opacity=0.2
            ),
            hoverinfo="none",
        )
    )

    fig.update_layout(
        mapbox={
            "accesstoken": "pk.eyJ1Ijoia25wb2JsZXRlIiwiYSI6ImNsZ2k5NGE4ZTBpc2IzY2xmemgwdWl5NXkifQ.UQ55c1MEZb-mQV-x66gKIQ",
            "style": "mapbox://styles/knpoblete/clgi3uciu001801ln65r8ors6",
            "zoom": 6,
            "center": {"lon": disasters["lon"][0], "lat": disasters["lat"][0]},
        },
        margin=dict(l=0, r=0, t=0, b=0),
        showlegend=False,
    )
    aspect_ratio = 1.15
    height = 350
    width = height * aspect_ratio
    img_bytes = fig.to_image(format="png", width=width, height=height)
    
    return img_bytes


def process_connectivity_df(df, h3_res, metric):
    print(df.shape)
    df = df.h3.h3_to_geo()
    df['lat'] = df['geometry'].x
    df['lon'] = df['geometry'].y
    df = df.h3.geo_to_h3_aggregate(h3_res,lat_col='lat',lng_col='lon',operation={metric: 'mean'}, return_geometry=False)
    print(df.shape)
    df.reset_index(inplace=True)
    h3_col = f'h3_0{h3_res}'
    df.set_index(h3_col, inplace=True)
    df = df.h3.h3_to_geo_boundary()
    df.reset_index(inplace=True)
    
    geojson_obj = (hexagons_dataframe_to_geojson(df,
                                                 hex_id_field=h3_col,
                                                 value_field=metric,
                                                 geometry_field='geometry'))
    topleft = df[h3_col].min()
    bottomright = df[h3_col].max()

    lat, lon = get_map_centroid(topleft, bottomright)

    return df, geojson_obj, lat, lon, h3_col

def hexagons_dataframe_to_geojson(df_hex, hex_id_field,geometry_field, value_field,file_output = None):
    list_features = []

    for i, row in df_hex.iterrows():
        feature = Feature(geometry = row[geometry_field],
                          id = row[hex_id_field],
                          properties = {"value": row[value_field]})
        list_features.append(feature)
    feat_collection = FeatureCollection(list_features)

    if file_output is not None:
        with open(file_output, "w") as f:
            json.dump(feat_collection, f)
    else :
          return feat_collection
    
def get_map_centroid(topleft, bottomright):
    topleft_latlng = h3.h3_to_geo(topleft)
    bottomright_latlng = h3.h3_to_geo(bottomright)
    lat = (topleft_latlng[0]+bottomright_latlng[0])/2
    lon = (bottomright_latlng[1]+topleft_latlng[1])/2
    return lat, lon

def generate_json_file(df, geojson_obj, lat, lon, h3_col, metric):
    event_json = {
        h3_col: df[h3_col].to_list(),
        'metric': df[metric].to_list(),
        'geojson_obj': geojson_obj,
        'lat': lat,
        'lon': lon,
        'style': 'mapbox://styles/knpoblete/clgi3uciu001801ln65r8ors6',
        'mapboxAccessToken': 'pk.eyJ1Ijoia25wb2JsZXRlIiwiYSI6ImNsZ2k5NGE4ZTBpc2IzY2xmemgwdWl5NXkifQ.UQ55c1MEZb-mQV-x66gKIQ'
    }

    return event_json

def plot_map(df, h3, color_col, geojson_obj, lat, lon, colorscale):
    plotly_map = (px.choropleth_mapbox(
                    df, 
                    geojson=geojson_obj, 
                    locations=h3, 
                    color=color_col,
                    color_continuous_scale=colorscale,
                    range_color=(0, 1),            
                    opacity=0.7,
                    ))
    
    plotly_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0},
                      mapbox={'accesstoken':"pk.eyJ1Ijoia25wb2JsZXRlIiwiYSI6ImNsZ2k5NGE4ZTBpc2IzY2xmemgwdWl5NXkifQ.UQ55c1MEZb-mQV-x66gKIQ",
                              'style': 'mapbox://styles/knpoblete/clgi3uciu001801ln65r8ors6',
                              'zoom': 6,
                              'center': {'lat': lat, 'lon': lon},},
                              showlegend = False)
    return plotly_map

def convert_to_img(fig):
    img_bytes = fig.to_image(format="png") 
    encoding = b64encode(img_bytes).decode()
    img_b64 = "data:image/png;base64," + encoding
    return img_b64

def transform_to_plotly_format(event_id, df):

    metric = 'avg_p_connectivity'
    df.set_index('h3_08', inplace=True)
    df = df[[metric]]
    df = df.dropna() # or df.fillna(1)
    df, geojson_obj, lat, lon, h3_col = process_connectivity_df(df, 7, metric) 
    json = generate_json_file(df, geojson_obj, lat, lon, h3_col, metric)
    colorscale = [[0, 'rgba(0, 131, 62, 0.5)'], 
                  [0.5, 'rgba(255, 194, 14, 0.5)'],
                  [1, 'rgba(226, 35, 26, 0.5)']]
    
    connectivity_fig = plot_map(df, 
                                  h3=h3_col, 
                                  color_col=metric, 
                                  geojson_obj=geojson_obj,
                                  lat=lat, 
                                  lon=lon, 
                                  colorscale=colorscale)

    aspect_ratio = 1.5
    height = 800
    width = height * aspect_ratio
    img_bytes = connectivity_fig.to_image(format="png", width=width, height=height, scale=6)

    return json, img_bytes 