from datetime import datetime

import pytz
import typer
from langchain.chains.llm import LLMChain
from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_openai import OpenAI
from typing_extensions import Annotated
from fundai.scraper import get_all_links, process_page, post_process_pages
from fundai.db import init_search_urls, DatabaseClient, load_config
from fundai.utils import prompt_template
import logging
from psycopg2.extras import Json, execute_values
import json

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


def obtain_schema_and_push(page: str, url: str, chain, db):

    split_p = post_process_pages(page, url)
    schema = chain.invoke(split_p)["text"]
    print(schema)
    insert_query = f"""
         INSERT INTO raw_property_listings (url, raw_data) 
         VALUES ('{url}', {Json(schema)});
     """
    logger.info(f"Schema for {url} extracted")

    with db.conn.cursor() as conn:
        conn.execute(insert_query)
    logger.info(f"Schema for {url} pushed")


@app.command()
def structure_data(
    home_type: Annotated[str, typer.Argument()] = "koop",
    area: Annotated[str, typer.Argument()] = "rotterdam",
):
    db = DatabaseClient(load_config())
    query = f"""
       SELECT 
           url
           ,page_content
       from raw_page_content 
       WHERE url LIKE '%/{home_type}/{area}/%' 
       """
    pages = db.read(query)
    prompt = PromptTemplate(
        input_variables=["question"],
        template=prompt_template,
    )
    # Create llm chain
    llm_chain = LLMChain(llm=OpenAI(model="gpt-3.5-turbo-instruct"), prompt=prompt)
    complete_chain = {"question": RunnablePassthrough()} | llm_chain
    for p in pages:
        obtain_schema_and_push(p[1], p[0], complete_chain, db)


@app.command()
def init_db():
    """
    Initialze the databse
    :return:
    """
    init_search_urls()
    logger.info("Database initialized")


if __name__ == "__main__":
    app()
