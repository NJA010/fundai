from datetime import datetime

import pytz
import typer
from typing_extensions import Annotated

from fundai.scraper import get_all_links, process_page, parse_schema
from fundai.db import init_search_urls, DatabaseClient, load_config
import logging

logging.basicConfig(level=logging.INFO)
app = typer.Typer()
logger = logging.getLogger(__name__)


@app.command()
def scrape_house_urls(
    home_type: Annotated[str, typer.Argument()] = "koop",
    area: Annotated[str, typer.Argument()] = "rotterdam",
):
    """
    Obtain all house URLs across all search pages
    :param home_type:
    :param area:
    :return:
    """

    urls = get_all_links(home_type, area)
    db = DatabaseClient(load_config())
    query = f"""
    SELECT 
        url 
    from search_page_urls 
    WHERE url LIKE '%/{home_type}/{area}/%' 
    """
    current_urls = {row[0] for row in db.read(query)}
    new_urls = urls - current_urls
    logger.info(f"Inserting {len(new_urls)} new URLs")
    if new_urls:
        amsterdam_tz = pytz.timezone("Europe/Amsterdam")
        ts = datetime.now(amsterdam_tz)
        values = [(ts, url) for url in new_urls]
        columns = ["date", "url"]
        db.insert_values(values, "search_page_urls", columns)
    logger.info("Successfully inserted all urls")


@app.command()
def scrape_house_pages(
    home_type: Annotated[str, typer.Argument()] = "koop",
    area: Annotated[str, typer.Argument()] = "rotterdam",
):
    db = DatabaseClient(load_config())
    query = """
        with all_ruls as (
        SELECT 
            url 
        from search_page_urls 
        WHERE url LIKE '%/{home_type}/{area}/%'
        ),
        processed_urls as (
        SELECT 
            url
        from raw_page_content
        WHERE url LIKE '%/{home_type}/{area}/%'
        ) 
        SELECT 
         all_urls.url
        FROM all_urls
        WHERE all_urls.url NOT IN (select url from processed_urls)
    """

    pages_to_scrape = {row[0] for row in db.read(query)}
    if not pages_to_scrape:
        logger.info("DB is up to date, all pages in seach_page_urls have been scraped")
        return
    logger.info(f"Obtaining page content {home_type}/{area} for {len(pages_to_scrape)}")
    pages = process_page(pages_to_scrape)
    values = [[p.metadata["source"], p.page_content] for p in pages]
    logger.info("Inserting page content...")
    db.insert_values(values, "raw_page_content", ["url", "page_content"])
    logger.info("Successfully inserted all page content")


@app.command()
def parse_house_pages(
    home_type: Annotated[str, typer.Argument()] = "koop",
    area: Annotated[str, typer.Argument()] = "rotterdam",
):
    db = DatabaseClient(load_config())
    parse_schema(home_type, area, db)


@app.command()
def init_db():
    """
    Initialze the databse
    :return:
    """
    db = DatabaseClient(load_config())
    init_search_urls(db)
    logger.info("Database initialized")


if __name__ == "__main__":
    app()
