from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    ForeignKey,
    Date,
    TIMESTAMP)

from datetime import date
from pydantic import BaseModel, Field
from pydantic.schema import Optional

from pydantic import BaseModel, Field, validator
from datetime import date

from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class Disaster(Base):
    __tablename__ = "disasters"
    event_id = Column(Integer, primary_key=True, index=True)
    #event_id = Column(String, primary_key=True, index=True)
    fromdate = Column(Date)
    todate = Column(Date)
    alertscore = Column(Float)
    name = Column(String)
    country = Column(String, nullable=True)
    rss_url = Column(String)
    geo_url = Column(String)
    country = Column(String, nullable=True)
    htmldescription = Column(String, nullable=True)


    @validator("fromdate", "todate", pre=True)
    def parse_date(cls, value):
        if isinstance(value, str):
            return date.fromisoformat(value)
        return value


class DisastersHex(Base):
    __tablename__ = "disasters_hex"
    event_id = Column(
        Integer, ForeignKey("disasters.event_id"), primary_key=True, index=True
    )
    h3_08 = Column(String(255))


class Adm2hex(Base):
    __tablename__ = "adm2_hex"
    h3_08 = Column(String(255), primary_key=True, index=True)
    gid0 = Column(String(255))
    gid1 = Column(String(255))
    gid2 = Column(String(255))
    name0 = Column(String(255))
    name1 = Column(String(255))
    name2 = Column(String(255))


class Eaproadm2(Base):
    __tablename__ = "eapro_adm2"
    #UID: int = Column(Integer, primary_key=True, index=True)
    GID_0: str = Column(String(255),primary_key=True, index=True)
    NAME_0: str = Column(String(255))
    GID_1: str = Column(String(255))
    NAME_1: str = Column(String(255))
    GID_2: str = Column(String(255))
    NAME_2: str = Column(String(255))
    COUNTRY: str = Column(String(255))
    #CONTINENT: str = Column(String(255))


class MetaConnectivityHex(Base):
    __tablename__ = "meta_connectivity_hex"
    meta_disaster_id: int = Column(Integer, primary_key=True, index=True)
    country: str = Column(String(255))
    data_type: enumerate = Column(String(255))
    date: date = Column(Date)
    h3_08: str = Column(String(255))
    update_date: TIMESTAMP = Column(TIMESTAMP)
    value: float = Column(Float)


class MetaConnectivityResponse(BaseModel):
    h3_08: str = Field(..., example="8a2a107bfffffff")
    week_start_date: date = Field(..., example="2021-01-01")
    avg_coverage: Optional[float] = Field(..., example=0.5)
    avg_no_coverage: Optional[float] = Field(..., example=0.5)
    avg_p_connectivity: Optional[float] = Field(..., example=0.5)


class DisasterResponse(BaseModel):
    event_id: int = Field(..., example=1000853)
    #event_id: str = Field(..., example=1000853)
    fromdate: date = Field(..., example="2022-01-01")
    todate: date = Field(..., example="2023-01-01")
    alertscore: float = Field(..., example=3.7)
    name: str = Field(..., example="Tropical Cyclone RAI-21")
    country: Optional[str] = Field(None, example="Philippines")
    rss_url: str = Field(
        ...,
        example="https://www.gdacs.org/contentdata/resources/TC/1000853/cap_1000853.xml",
    )
    geo_url: str = Field(
        ...,
        example="https://www.gdacs.org/gdacsapi/api/polygons/getgeometry?eventtype=TC&eventid=1000853",
    )


class Population(Base):
    __tablename__ = "population_crosstab"
    h3_08 = Column(String, primary_key=True, index=True)
    children_under_five = Column(Float)
    elderly_60_plus = Column(Float)
    men = Column(Float)
    women = Column(Float)
    women_of_reproductive_age_15_49 = Column(Float)
    youth_15_24 = Column(Float)
    general = Column(Float)
    country = Column(String)


class PopulationResponse(BaseModel):
    name: Optional[str] = Field(None, example="Cebu City")
    gid2: Optional[str] = Field(None, example="PHL.1.1_1")
    children_under_five: int
    elderly_60_plus: int
    men: int
    women: int
    women_of_reproductive_age_15_49: int
    youth_15_24: int
    general: int


class Hospitals(Base):
    __tablename__ = "hospitals"
    h3_08 = Column(String, primary_key=True, index=True)
    count = Column(Integer)


class Schools(Base):
    __tablename__ = "schools"
    h3_08 = Column(String, primary_key=True, index=True)
    count = Column(Integer)


class FacilityResponse(BaseModel):
    hospitals: int = Field(..., example=10)
    schools: int = Field(..., example=20)


class MovementResponse(BaseModel):
    origin: str = Field(..., example="Manila")
    destination: str = Field(..., example="Cebu")
    total: int = Field(..., example=1000)

class ConnectivityFormattedResponse(BaseModel):
    h3_col: list = Field(..., example=[])
    metric: list = Field(..., example=[])
    geojson_obj: dict = Field(..., example={})
    lat: int = Field(..., example=1000)
    lon: int = Field(..., example=1000)
    style: str = Field(..., example='')
    mapboxAccessToken: str = Field(..., example='')
