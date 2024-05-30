import json
from typing import Sequence, Any
import nest_asyncio
from langchain.chains.llm import LLMChain
from langchain_community.document_loaders import AsyncChromiumLoader
import logging
import re

from langchain_community.document_transformers import Html2TextTransformer
from langchain_core.documents import Document
from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import RunnablePassthrough
from openai import OpenAI
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter
from fundai.db import DatabaseClient
from fundai.utils import prompt_template

logger = logging.getLogger(__name__)

ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.183 Safari/537.36"


class AsyncChromiumLoaderHeader(AsyncChromiumLoader):
    async def ascrape_playwright(self, url: str) -> str:
        """
        Asynchronously scrape the content of a given URL using Playwright's async API.

        Args:
            url (str): The URL to scrape.

        Returns:
            str: The scraped HTML content or an error message if an exception occurs.

        """
        from playwright.async_api import async_playwright

        logger.info("Starting scraping...")
        results = ""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            try:
                page = await browser.new_page(user_agent=ua)
                await page.goto(url)
                results = await page.content()  # Simply get the HTML content
                logger.info("Content scraped")
            except Exception as e:
                results = f"Error: {e}"
            await browser.close()
        return results


def post_process_pages(page: str, url: str) -> str:
    start = page.find("Bewaren")
    end = page.find("#### Wat is jouw huis waard?")
    if start == -1 or end == -1:
        logger.error("Start and end not found, returning original docs")
    else:
        logger.info(f"slicing document {url} from {start} to {end}")
        page = page[start:end].replace("\n", "")
    return page


def get_max_page(page: str) -> int:
    """
    Based on a page, returns the maximum of available pages
    :param page:
    :return:
    """
    volgende = page.find("* Volgende")
    vorige = page.find("* Vorige")
    n_pages = page[vorige:volgende].replace("\n", "").split(" ")
    n_pages_int = []
    for p in n_pages:
        try:
            n_pages_int.append(int(p))
        except ValueError:
            continue

    return max(n_pages_int)


def get_links_from_page(page: str, home_type: str, area: str) -> set[str]:
    """
    With regex, find all URLs on a page full of text
    :param page: text of associated page
    :param home_type: return URLs for this home_type
    :param area: return URLs for this area
    :return: unique URLs
    """
    results = re.findall(r"\((.*?)\)", page.replace("\n", ""))
    return {r for r in results if f"{home_type}/{area}" in r and "https" in r}


def process_page(urls: Sequence[str]) -> Sequence[Document]:
    """
    Using playwrigth, return the body of all URL in urls and convert to string
    :param urls: asynchronously scrapes these URls
    :return: sequence of langchain documents for all URLs
    """
    nest_asyncio.apply()
    # Scrapes the blogs above
    loader = AsyncChromiumLoaderHeader(urls)
    docs = loader.load()

    # # Converts HTML to plain text
    html2text = Html2TextTransformer(ignore_links=False)
    docs_transformed = html2text.transform_documents(docs)

    return docs_transformed


def get_all_links(home_type: str, area: str) -> set[str]:
    """
    Starting on the main search page, this returns the URLs of
    all houses of every search pages, from one to max_pages
    :param home_type: return URLs for this home_type
    :param area: return URLs for this area
    :return: URls of all
    """
    start_url = f"https://www.funda.nl/zoeken/{home_type}?selected_area=%5B%22{area}%22%5D&search_result=1"
    page = process_page([start_url])
    max_pages = get_max_page(page[0].page_content)
    logger.info(
        f"Finished with main search page, will now scrape {max_pages} remaining search pages"
    )
    all_urls = [
        f"https://www.funda.nl/zoeken/{home_type}?selected_area=%5B%22{area}%22%5D&search_result={i}"
        for i in range(1, max_pages + 1)
    ]
    all_pages = process_page(all_urls)
    logger.info("All search pages have been scraped")
    fetched_links = set()
    for p in all_pages:
        fetched_links = fetched_links.union(
            get_links_from_page(p.page_content, home_type, area)
        )
    logger.info(
        f"{len(fetched_links)} house links have been found on /{home_type}/{area}"
    )
    return fetched_links


def obtain_schema_openai(page_content: str, url: str, client) -> dict[str, Any]:

    completion = client.chat.completions.create(
        model="gpt-3.5-turbo-1106",
        messages=[
            {"role": "system", "content": prompt_template},
            {"role": "user", "content": page_content},
        ],
    )
    logger.info(f"Tokens used: {completion.usage}")
    schema_string = completion.choices[0].message.content
    schema_string = (
        schema_string.replace("```json", "")
        .replace("```", "")
        .replace('"\n ', '",\n ')
        .replace("\n", "")
    )
    try:
        # fix erroneous JSON format
        schema = json.loads(schema_string)
    except Exception as err:
        print(schema_string[:5])
        print(schema_string[-5:])
        print(schema_string)

        raise err

    logger.info(f"Schema for {url} extracted")
    return schema


def obtain_schema_and_push(page: str, url: str, db: DatabaseClient, *args, **kwargs):
    """
    Post process page contents, extract data schema using chain and insert into db
    """
    split_p = post_process_pages(page, url)
    i = 0
    while i < 10:
        try:
            schema = obtain_schema_openai(split_p, url, *args, **kwargs)
            i = 11
        except Exception as err:
            logger.error(f"Parsing data schema failed, retry number: {i}", err)
            i += 1
    insert_query = f"""
         INSERT INTO raw_property_listings (url, raw_data) 
         VALUES ('{url}', %s);
     """

    with db.conn.cursor() as conn:
        conn.execute(insert_query, [Json(schema)])
    logger.info(f"Schema for {url} pushed successfully")


def parse_schema(home_type: str, area: str, db: DatabaseClient):
    query = f"""
       SELECT 
           url
           ,page_content
       from raw_page_content 
       WHERE url LIKE '%/{home_type}/{area}/%' AND url NOT in (SELECT url FROM raw_property_listings)
       """
    pages = db.read(query)
    logger.info(f"Will try to parse {len(pages)} pages")
    # Create llm client
    client = OpenAI()
    for p in pages:
        obtain_schema_and_push(p[1], p[0], db, client)
