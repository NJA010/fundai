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

    def execute(self, query: str):
        with self.conn.cursor() as cur:
            cur.execute(query)


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
            url VARCHAR(255) UNIQUE,
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
    living_area INTEGER,
    bedrooms INTEGER,
    price INTEGER,
    price_per_m2 INTEGER,
    description TEXT,
    asking_price INTEGER,
    asking_price_per_m2 INTEGER,
    status TEXT,
    acceptance TEXT,
    vve_contribution NUMERIC,
    type_of_apartment TEXT,
    type_of_construction TEXT,
    year_of_construction INTEGER,
    accessibility TEXT,
    living_area_m2 INTEGER,
    volume INTEGER,
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
        url VARCHAR(255) UNIQUE,
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


def create_property_listings(db: DatabaseClient):
    query = """
    INSERT INTO property_listings
    SELECT 
        raw_data->>'address' AS address,
        raw_data->>'postal_code' AS postal_code,
        raw_data->>'city' AS city,
        raw_data->>'neighborhood' AS neighborhood,
        (raw_data->>'living_area')::INTEGER AS living_area,
        (raw_data->>'bedrooms')::INTEGER AS bedrooms,
        (raw_data->>'price')::INTEGER AS price,
        (raw_data->>'price_per_m2')::INTEGER AS price_per_m2,
        raw_data->>'description' AS description,
        (raw_data->>'asking_price')::INTEGER AS asking_price,
        (raw_data->>'asking_price_per_m2')::INTEGER AS asking_price_per_m2,
        raw_data->>'status' AS status,
        raw_data->>'acceptance' AS acceptance,
        (raw_data->>'vve_contribution')::NUMERIC AS vve_contribution,
        raw_data->>'type_of_apartment' AS type_of_apartment,
        raw_data->>'type_of_construction' AS type_of_construction,
        (raw_data->>'year_of_construction')::INTEGER AS year_of_construction,
        raw_data->>'accessibility' AS accessibility,
        (raw_data->>'living_area_m2')::INTEGER AS living_area_m2,
        (raw_data->>'volume')::INTEGER AS volume,
        (raw_data->>'number_of_rooms')::INTEGER AS number_of_rooms,
        (raw_data->>'number_of_bedrooms')::INTEGER AS number_of_bedrooms,
        (raw_data->>'number_of_bathrooms')::INTEGER AS number_of_bathrooms,
        raw_data->>'bathroom_facilities' AS bathroom_facilities,
        (raw_data->>'number_of_floors')::INTEGER AS number_of_floors,
        raw_data->>'located_on' AS located_on,
        raw_data->>'facilities' AS facilities,
        raw_data->>'energy_label' AS energy_label,
        raw_data->>'insulation' AS insulation,
        raw_data->>'heating' AS heating,
        raw_data->>'hot_water' AS hot_water,
        raw_data->>'boiler_brand' AS boiler_brand,
        raw_data->>'boiler_type' AS boiler_type,
        raw_data->>'boiler_ownership' AS boiler_ownership,
        raw_data->>'cadastral_number' AS cadastral_number,
        raw_data->>'ownership_status' AS ownership_status,
        raw_data->>'type_of_parking' AS type_of_parking,
        (raw_data->>'registered_with_chamber_of_commerce')::BOOLEAN AS registered_with_chamber_of_commerce,
        (raw_data->>'annual_meeting')::BOOLEAN AS annual_meeting,
        (raw_data->>'periodic_contribution')::BOOLEAN AS periodic_contribution,
        (raw_data->>'reserve_fund')::BOOLEAN AS reserve_fund,
        (raw_data->>'maintenance_plan')::BOOLEAN AS maintenance_plan,
        (raw_data->>'building_insurance')::BOOLEAN AS building_insurance,
        raw_data->>'agency_name' AS agency_name,
        raw_data->>'phone_number' AS phone_number
    FROM raw_property_listings 
    """
    db.execute(query)
