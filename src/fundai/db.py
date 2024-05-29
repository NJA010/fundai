from datetime import datetime
from configparser import ConfigParser
import pytz
import psycopg2
from psycopg2.extras import Json, execute_values
import logging
from typing import Any, Sequence

logger = logging.getLogger(__name__)


def load_config(filename="database.ini", section="postgresql") -> dict[str, str]:
    parser = ConfigParser()
    parser.read(filename)

    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception(
            "Section {0} not found in the {1} file".format(section, filename)
        )

    return config


def connect(config: dict[str, str]):
    with psycopg2.connect(**config) as conn:
        print("Connected to the PostgreSQL server.")
        return conn


class DatabaseClient:
    def __init__(self, config: dict[str, str]) -> None:
        self.conn = connect(config)
        logger.info("Connection to postgres database successful")
        self.conn.autocommit = True

    def init(self, query: str):
        with self.conn.cursor() as cur:
            cur.execute(query)
            self.conn.commit()

    def insert_values(
        self, values: Sequence[Any], table_name: str, column_names: list[str]
    ):
        with self.conn.cursor() as cur:
            columns_str = ", ".join(column_names)
            query = f"INSERT INTO {table_name} ({columns_str}) VALUES (%s, %s);"
            cur.executemany(query, values)
            self.conn.commit()

    def read(self, query: str) -> dict:
        with self.conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()


def init_search_urls(db: DatabaseClient):
    search_page_urls = """
    CREATE TABLE IF NOT EXISTS search_page_urls 
        (
            id SERIAL PRIMARY KEY,
            date TIMESTAMP,
            url VARCHAR(255) UNIQUE
        )
    """
    house_page_content = """
    CREATE TABLE IF NOT EXISTS raw_page_content
        (
            url VARCHAR(255),
            page_content TEXT,
            FOREIGN KEY (url) REFERENCES search_page_urls(url)
        )
    """
    house_data = """
    CREATE TABLE IF NOT EXISTS property_listings (
    id SERIAL PRIMARY KEY,
    address TEXT,
    postal_code TEXT,
    city TEXT,
    neighborhood TEXT,
    living_area TEXT,
    bedrooms INTEGER,
    price TEXT,
    price_per_m2 TEXT,
    description TEXT,
    asking_price TEXT,
    asking_price_per_m2 TEXT,
    status TEXT,
    acceptance TEXT,
    vve_contribution TEXT,
    type_of_apartment TEXT,
    type_of_construction TEXT,
    year_of_construction INTEGER,
    accessibility TEXT,
    living_area_m2 TEXT,
    volume TEXT,
    number_of_rooms INTEGER,
    number_of_bedrooms INTEGER,
    number_of_bathrooms INTEGER,
    bathroom_facilities TEXT,
    number_of_floors INTEGER,
    located_on TEXT,
    facilities TEXT,
    energy_label TEXT,
    insulation TEXT,
    heating TEXT,
    hot_water TEXT,
    boiler_brand TEXT,
    boiler_type TEXT,
    boiler_ownership TEXT,
    cadastral_number TEXT,
    ownership_status TEXT,
    type_of_parking TEXT,
    registered_with_chamber_of_commerce BOOLEAN,
    annual_meeting BOOLEAN,
    periodic_contribution BOOLEAN,
    reserve_fund BOOLEAN,
    maintenance_plan BOOLEAN,
    building_insurance BOOLEAN,
    agency_name TEXT,
    phone_number TEXT
    );
    """
    raw_property_listings = """
    CREATE TABLE IF NOT EXISTS raw_property_listings (
        id SERIAL PRIMARY KEY,
        url VARCHAR(255),
        raw_data JSONB,
        FOREIGN KEY (url) REFERENCES search_page_urls(url)
    );
    """
    db.init(raw_property_listings)
    logger.info("Created raw_property_listings table")
    db.init(house_data)
    logger.info("Created house table")
    db.init(house_page_content)
    logger.info("Created raw_page_content table")
    db.init(search_page_urls)
    logger.info("Created search_page_urls table")
