import asyncio
from io import BytesIO
import io
import json
import logging
import os
from dotenv import load_dotenv
import random

load_dotenv()


from starlette.responses import RedirectResponse
from fastapi import FastAPI, Depends

from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from fastapi.responses import Response
import uvicorn
import pickle

from sqlalchemy import and_, or_, case
from sqlalchemy import create_engine, func, desc, text
from sqlalchemy.orm import sessionmaker, Session

from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from fastapi_cache.decorator import cache

from redis import asyncio as aioredis

from helpers.create_map import generate_map, transform_to_plotly_format
from helpers.movement import dummy_make_movement

from datetime import timedelta


eapro_countries = [
    "Brunei",
    "Cambodia",
    "Cook Islands",
    "Fiji",
    "Indonesia",
    "Japan",
    "Kiribati",
    "Laos",
    "Malaysia",
    "Marshall Islands",
    "Micronesia",
    "Mongolia",
    "Myanmar",
    "Nauru",
    "New Zealand",
    "Niue",
    "Palau",
    "Papua New Guinea",
    "Philippines",
    "Samoa",
    "Singapore",
    "Solomon Islands",
    "Thailand",
    "Timor-Leste",
    "Tonga",
    "Tuvalu",
    "Vanuatu",
    "Viet Nam",
]


@cache()
async def get_cache():
    return 1


cache_timeout = os.getenv("CACHE_TIMEOUT", 1)

import h3pandas
import pandas as pd
#import geopandas as gpd
from typing import List
import datetime

# MODELS

from models.models import Disaster, DisastersHex, DisasterResponse, ConnectivityFormattedResponse
from models.models import Population, PopulationResponse, Eaproadm2, Adm2hex
from models.models import Hospitals, Schools, FacilityResponse
from models.models import MovementResponse
from models.models import MetaConnectivityHex, MetaConnectivityResponse

app = FastAPI()

# Database Connection
DATABASE_URL = (
    "postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_database}".format(
        db_user=os.getenv("DB_USERNAME"),
        db_password=os.getenv("DB_PASSWORD"),
        db_host=os.getenv("DB_HOST"),
        db_port=os.getenv("DB_PORT"),
        db_database=os.getenv("DB_DATABASE"),
    )
)
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Use the baseURL depending if run is local or not
baseURL = f"https://sitrepapi.azurewebsites.net"

@app.on_event("startup")
async def startup():
    redis = aioredis.from_url(
        os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        encoding="utf-8",
        decode_responses=True,
    )
    FastAPICache.init(RedisBackend(redis), prefix="fastapi-cache")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# HELPER FUNCTIONS - Move to Helpers
def get_pop_from_db(event_id, session):
    population = (
        session.query(
            func.sum(Population.children_under_five).label("children_under_five"),
            func.sum(Population.elderly_60_plus).label("elderly_60_plus"),
            func.sum(Population.men).label("men"),
            func.sum(Population.women).label("women"),
            func.sum(Population.women_of_reproductive_age_15_49).label(
                "women_of_reproductive_age_15_49"
            ),
            func.sum(Population.youth_15_24).label("youth_15_24"),
            func.sum(Population.general).label("general"),
        )
        .join(DisastersHex, Population.h3_08 == DisastersHex.h3_08)
        .filter(DisastersHex.event_id == event_id)
        .first()
    )
    response_model = PopulationResponse(
        children_under_five=population.children_under_five,
        elderly_60_plus=population.elderly_60_plus,
        men=population.men,
        women=population.women,
        women_of_reproductive_age_15_49=population.women_of_reproductive_age_15_49,
        youth_15_24=population.youth_15_24,
        general=population.general,
    )
    return response_model


def get_connectivity(event_id, session):

    filtered_disasters = (
        session.query(DisastersHex.h3_08)
        .filter(DisastersHex.event_id == event_id)
        .subquery()
    )

    latest_date = (
        session.query(func.max(MetaConnectivityHex.date))
        .select_from(MetaConnectivityHex)
        .join(
            filtered_disasters, MetaConnectivityHex.h3_08 == filtered_disasters.c.h3_08
        )
        .scalar()
    )

    filtered_meta = (
        session.query(
            MetaConnectivityHex.h3_08,
            MetaConnectivityHex.data_type,
            MetaConnectivityHex.date,
            MetaConnectivityHex.value,
        )
        .join(
            filtered_disasters, MetaConnectivityHex.h3_08 == filtered_disasters.c.h3_08
        )
        .filter(MetaConnectivityHex.date >= latest_date - timedelta(days=28))
        .group_by(
            MetaConnectivityHex.h3_08,
            MetaConnectivityHex.data_type,
            MetaConnectivityHex.date,
            MetaConnectivityHex.value,
        )
        .subquery()
    )

    pivoted_meta = (
        session.query(
            filtered_meta.c.h3_08,
            filtered_meta.c.date,
            func.date_trunc("week", filtered_meta.c.date).label("week_start_date"),
            func.sum(
                case(
                    [(filtered_meta.c.data_type == "coverage", filtered_meta.c.value)],
                    else_=None,
                )
            ).label("coverage"),
            func.sum(
                case(
                    [
                        (
                            filtered_meta.c.data_type == "no_coverage",
                            filtered_meta.c.value,
                        )
                    ],
                    else_=None,
                )
            ).label("no_coverage"),
            func.sum(
                case(
                    [
                        (
                            filtered_meta.c.data_type == "p_connectivity",
                            filtered_meta.c.value,
                        )
                    ],
                    else_=None,
                )
            ).label("p_connectivity"),
        )
        .group_by(filtered_meta.c.h3_08, filtered_meta.c.date)
        .subquery()
    )

    result = (
        session.query(
            pivoted_meta.c.h3_08,
            pivoted_meta.c.week_start_date,
            func.avg(pivoted_meta.c.coverage).label("avg_coverage"),
            func.avg(pivoted_meta.c.no_coverage).label("avg_no_coverage"),
            func.avg(pivoted_meta.c.p_connectivity).label("avg_p_connectivity"),
        )
        .group_by(pivoted_meta.c.h3_08, pivoted_meta.c.week_start_date)
        .order_by(pivoted_meta.c.h3_08, pivoted_meta.c.week_start_date)
        .all()
    )

    return result


def get_pop_from_db_by_region(event_id, session):
    population = (
        session.query(
            Adm2hex.gid2,
            Eaproadm2.NAME_2,
            func.sum(Population.children_under_five).label("children_under_five"),
            func.sum(Population.elderly_60_plus).label("elderly_60_plus"),
            func.sum(Population.men).label("men"),
            func.sum(Population.women).label("women"),
            func.sum(Population.women_of_reproductive_age_15_49).label(
                "women_of_reproductive_age_15_49"
            ),
            func.sum(Population.youth_15_24).label("youth_15_24"),
            func.sum(Population.general).label("general"),
        )
        .join(DisastersHex, Population.h3_08 == DisastersHex.h3_08)
        .filter(DisastersHex.event_id == event_id)
        .join(Adm2hex, DisastersHex.h3_08 == Adm2hex.h3_08)
        .join(Eaproadm2, Adm2hex.gid2 == Eaproadm2.GID_2)
        .group_by(Adm2hex.gid2, Eaproadm2.NAME_2)
        .order_by(desc("children_under_five"))
        .limit(7)
    ).all()
    response = list()
    for pop in population:
        response_model = PopulationResponse(
            gid2=pop.gid2,
            name=pop.NAME_2,
            children_under_five=pop.children_under_five,
            elderly_60_plus=pop.elderly_60_plus,
            men=pop.men,
            women=pop.women,
            women_of_reproductive_age_15_49=pop.women_of_reproductive_age_15_49,
            youth_15_24=pop.youth_15_24,
            general=pop.general,
        )
        response.append(response_model)
    return response


def stack_df(grouped_df):
    out = grouped_df.drop("gid2", axis=1).set_index("datatype").transpose()
    return out


def get_hexes_from_db(event_id, session):
    hex_ids = (
        session.query(DisastersHex.h3_08)
        .filter(DisastersHex.event_id == event_id)
        .all()
    )
    hex_ids = [h3[0] for h3 in hex_ids]
    if len(hex_ids) < 10:
        raise ValueError("Too few hex ids")
    return hex_ids


def make_hexes(geometry):
    hex_ids = h3pandas.h3.polyfill_resample(geometry, 8, return_geometry=False)
    return hex_ids


def add_hexes_to_db(event_id):
    logging.info("No hex ids found, making them")
    sql_query = f"""SELECT event_id, eventtype, geometry_validated AS geometry from disasters where event_id = '{event_id}'"""
    disaster_df = gpd.read_postgis(sql_query, engine, geom_col="geometry")
    disaster_df = disaster_df.h3.polyfill_resample(8, return_geometry=False)
    disaster_df = (
        disaster_df.reset_index()[["eventtype", "event_id", "h3_polyfill"]]
        .set_index("event_id")
        .rename(columns={"h3_polyfill": "h3"})
    )
    disaster_df.to_sql("disasters_hex", engine, if_exists="append", index=True)
    return True


# Routes - move to routes.py


@app.get(
    "/disaster/{event_id}/image.png",
    responses={200: {"content": {"image/png": {}}}},
    response_class=Response,
)
async def get_disaster_image(event_id: int, session: Session = Depends(get_db)):
    filename = f"img_{event_id}.pickle"
    if os.path.exists(filename):
        with open(filename, "rb") as f:
            map_image = pickle.load(f)
    else:
        map_image = generate_map(event_id, engine)
        with open(filename, "wb") as f:
            pickle.dump(map_image, f)
    resp = Response(content=map_image, media_type="image/png")
    return resp


@app.get("/disaster", response_model=List[DisasterResponse])
@cache(expire=cache_timeout)
async def get_all_disasters(
    session: Session = Depends(get_db),
):
    disasters = session.query(Disaster).order_by(desc("fromdate")).limit(20)
    response = list()
    for disaster in disasters:
        response.append(
            DisasterResponse(
                event_id=disaster.event_id,
                fromdate=disaster.fromdate,
                todate=disaster.todate,
                alertscore=disaster.alertscore,
                #name=disaster.name,
                name=disaster.htmldescription,
                country=disaster.country,
                rss_url=disaster.rss_url,
                geo_url=disaster.geo_url,
            )
        )
    return response


@app.get(
    "/disaster/{event_id}/connectivity", response_model=List[MetaConnectivityResponse]
)
@cache(expire=cache_timeout)
async def get_disaster_connectivity(event_id: int, session: Session = Depends(get_db)):
    filename = f"connectivity_{event_id}.pkl"
    connectivity = get_connectivity(event_id, session)
    response = list()
    connectivity_json = list()
    for hex in connectivity:
        response.append(
            MetaConnectivityResponse(
                h3_08=hex.h3_08,
                week_start_date=hex.week_start_date,
                avg_coverage=hex.avg_coverage,
                avg_no_coverage=hex.avg_no_coverage,
                avg_p_connectivity=hex.avg_p_connectivity,
            )
        )
        connectivity_json_hex = {
            'h3_08':hex.h3_08,
            # 'week_start_date':hex.week_start_date,
            'avg_coverage':hex.avg_coverage,
            'avg_no_coverage':hex.avg_no_coverage,
            'avg_p_connectivity':hex.avg_p_connectivity,
        }
        connectivity_json.append(connectivity_json_hex)

    save_file = open(f"connectivity_{event_id}.json", "w")  
    json.dump(connectivity_json, save_file, indent = 4)  
    save_file.close()  

    return response

@app.get("/v0/disaster", tags=["deprecated"])
@cache(expire=cache_timeout)
async def get_sitrep(
    session: Session = Depends(get_db),
):
    # disasters = session.query(Disaster).order_by(desc("fromdate")).limit(20)
    disasters = session.query(Disaster).filter(Disaster.fromdate > "2022-05-01")
    good_ones = [1000922, 1000965, 1359959, 1353717, 1000057, 1101710, 1353543, 1101875,1367745,1000961,1000966,1347295,1356811,1101778,1353717,1347183, 1343617, 1000937, 1000915, 1337179]
    response = list()
    for disaster in disasters:
        if disaster.event_id in good_ones:
            this_disaster = dict()
            this_disaster["id"] = disaster.event_id
            this_disaster["name"] = disaster.htmldescription
            #this_disaster["name"] = disaster.name
            # this_disaster[
            #     "src_img"
            # ] = "https://www.theinspiration.com/wp-content/uploads/cartographie-densite-population-rayshader-GT11-1920x1680-1.webp"
            this_disaster[
                "src_img" ] = f"{baseURL}/disaster/{disaster.event_id}/image.png"
                #"src_img" ] = f"https://sitrep-api.unicef.datafordecisionmaking.com/disaster/{disaster.event_id}/image.png"
            response.append(this_disaster)
    with open("sitrep_cache.pkl", "wb") as f:
        pickle.dump(response, f)
    return response


@app.get("/v0/disaster/{event_id}", tags=["deprecated"])
@cache(expire=cache_timeout)
async def get_sitrep_by_id(event_id: int, session: Session = Depends(get_db)):

    disaster = session.query(Disaster).filter(Disaster.event_id == event_id).first()
    headline_population = get_pop_from_db(event_id, session)

    population_dict = headline_population.dict()
    if "general" in population_dict:
        population_dict["total"] = population_dict["men"] + population_dict["women"]
    else:
        population_dict["total"] = population_dict["men"] + population_dict["women"]

    # POPULATION BY REGION
    population = get_pop_from_db_by_region(event_id, session)
    totals = [p.dict() for p in population]
    i = 1000
    for total in totals:
        total["id"] = i
        total["rwi"] = random.randrange(-200, 200) / 100
        i += 1
    response = dict(
        {
            "id": disaster.event_id,
            "name": disaster.htmldescription,
            #"name": disaster.name,
            "total": population_dict["total"],
            "youth": population_dict["youth_15_24"],
            "children_under_five": population_dict["children_under_five"],
            "women_of_reproductive_age": population_dict[
                "women_of_reproductive_age_15_49"
            ],
            "elderly": population_dict["elderly_60_plus"],
            "men": population_dict["men"],
            "women": population_dict["women"],
#            "src_img": f"https://sitrep-api.unicef.datafordecisionmaking.com/disaster/{disaster.event_id}/image.png",
            "src_img": f"{baseURL}/disaster/{disaster.event_id}/image.png",
            "areas": totals,
        },

    )
    movements = dummy_make_movement(response["areas"])
    mov_response = list()
    for movement in movements:
        mov_response.append(
            {
                "source": movement["origin"],
                "target": movement["destination"],
                "count": movement["total"],
            }
        )
    response["movements"] = mov_response

    filename = f"sitrep_{event_id}.pkl"
    if not os.path.exists(filename):
        with open(filename, "wb") as f:
            pickle.dump(response, f)
    return response


@app.get(
    "/disaster/{event_id}",
    response_model=DisasterResponse,
)
@cache(expire=cache_timeout)
async def get_disaster_by_id(event_id: int, session: Session = Depends(get_db)):

    disaster = session.query(Disaster).filter(Disaster.event_id == event_id).first()
    response_model = DisasterResponse(
        event_id=disaster.event_id,
        fromdate=disaster.fromdate,
        todate=disaster.todate,
        alertscore=disaster.alertscore,
        #name=disaster.name,
        name=disaster.htmldescription,
        country=disaster.country,
        rss_url=disaster.rss_url,
        geo_url=disaster.geo_url,
    )
    return response_model


@app.get(
    "/population/{event_id}", response_model=PopulationResponse, tags=["population"]
)
@cache(expire=cache_timeout)
async def get_population_by_id(event_id: int, session: Session = Depends(get_db)):
    population = get_pop_from_db(event_id, session)
    return population


@app.get("/facilities/{event_id}", response_model=FacilityResponse, tags=["facilities"])
@cache(expire=cache_timeout)
async def get_facilities_by_id(event_id: int, session: Session = Depends(get_db)):
    hospitals = (
        session.query(func.sum(Hospitals.count).label("total_hospitals"))
        .join(DisastersHex, Hospitals.h3_08 == DisastersHex.h3_08)
        .filter(DisastersHex.event_id == event_id)
        .first()
    )
    schools = (
        session.query(func.sum(Schools.count).label("total_schools"))
        .join(DisastersHex, Schools.h3_08 == DisastersHex.h3_08)
        .filter(DisastersHex.event_id == event_id)
        .first()
    )

    response_model = FacilityResponse(
        schools=schools.total_schools,
        hospitals=hospitals.total_hospitals,
    )
    return response_model


@app.get(
    "/movement/{event_id}", response_model=List[MovementResponse], tags=["movement"]
)
@cache(expire=cache_timeout)
async def get_movement_by_id(event_id: int, session: Session = Depends(get_db)):
    movements = list()
    movements.append({"origin": "Manila", "destination": "Quezon City", "total": 100})
    movements.append({"origin": "Manila", "destination": "Cebu", "total": 500})
    movements.append({"origin": "Quezon City", "destination": "Cebu", "total": 500})
    movements.append({"origin": "Quezon City", "destination": "Baguio", "total": 600})

    response = list()
    for movement in movements:
        response.append(
            MovementResponse(
                origin=movement["origin"],
                destination=movement["destination"],
                total=movement["total"],
            )
        )
    return response


@app.get(
    "/population/by_region/{event_id}",
    response_model=List[PopulationResponse],
    tags=["population"],
)
@cache(expire=cache_timeout)
async def get_population_by_region(event_id: int, session: Session = Depends(get_db)):
    population = get_pop_from_db_by_region(session, event_id)
    return population


@app.get("/demo/disaster")
def return_demo(session: Session = Depends(get_db)):
    good_ones = [1000922, 1000965, 1359959, 1353717, 1000057, 1101710, 1353543, 1101875,1367745,1000961,1000966,1347295,1356811,1101778,1353717,1347183, 1343617, 1000937, 1000915, 1337179]
    with open("sitrep_cache.pkl", "rb") as f:
        sitrep_list = pickle.load(f)
    return sitrep_list


@app.get("/demo/disaster/{event_id}")
def return_demo_individual(event_id: int, session: Session = Depends(get_db)):
    filename = f"sitrep_{event_id}.pkl"
    # if the file with filename exists, return it
    if os.path.exists(filename):
        with open(filename, "rb") as f:
            sitrep = pickle.load(f)
        return sitrep
    # else, redirect to /v0/disaster/{event_id}
    else:
        return RedirectResponse(url=f"/v0/disaster/{event_id}")

@app.get("/demo/disaster/{event_id}/connectivity_formatted", response_model=ConnectivityFormattedResponse)
@cache(expire=cache_timeout)
async def get_connectivity_data(
    event_id: int,
    session: Session = Depends(get_db),
): 
    filename = f'connectivity_{event_id}.json'
    if os.path.exists(filename):
        print("it exists!")
        df = pd.read_json(filename)
        connectivity, con_image = transform_to_plotly_format(event_id, df)
        response = ConnectivityFormattedResponse(
            h3_col=connectivity['h3_07'],
            metric=connectivity['metric'],
            geojson_obj=connectivity['geojson_obj'],
            lat=connectivity['lat'],
            lon=connectivity['lon'],
            style=connectivity['style'],
            mapboxAccessToken=connectivity['mapboxAccessToken'],
        )
        return response
    else:
        print("it does not exist")
        await get_disaster_connectivity(event_id, session)
        df = pd.read_json(filename)
        connectivity, con_image = transform_to_plotly_format(event_id, df)
        response = ConnectivityFormattedResponse(
            h3_col=connectivity['h3_07'],
            metric=connectivity['metric'],
            geojson_obj=connectivity['geojson_obj'],
            lat=connectivity['lat'],
            lon=connectivity['lon'],
            style=connectivity['style'],
            mapboxAccessToken=connectivity['mapboxAccessToken'],
        )
        return response

@app.get("/demo/disaster/{event_id}/connectivity.png", responses={200: {"content": {"image/png": {}}}},
    response_class=Response,)
async def generate_connectivity_image(
    event_id: int,
    session: Session = Depends(get_db),
):
    df = pd.read_json(f'connectivity_{event_id}.json')
    connectivity, con_image = transform_to_plotly_format(event_id, df)
    filename = f"img.pickle"
    with open(filename, "wb") as f:
        pickle.dump(con_image, f)
    resp = Response(content=con_image, media_type="image/png")
    return resp


# Uncomment this to run the app locally / dev mode


origins = ["*"]
app = CORSMiddleware(
     app=app,
     allow_origins=origins,
     allow_credentials=True,
     allow_methods=["*"],
     allow_headers=["*"],
)

# if __name__ == "__main__":
#      uvicorn.run("main:app", host="localhost", port=8000, reload=True)