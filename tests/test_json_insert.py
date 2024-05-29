from openai import OpenAI

from fundai.db import DatabaseClient, load_config
from fundai.scraper import obtain_schema_and_push
from mock import MagicMock


def get_raw_page_content() -> str:
    with open("./tests/raw_page_content.txt", "r") as f:
        page = f.read()
    return page


def get_llm_response() -> dict[str, str]:
    with open("./tests/structured_page_content.txt", "r") as f:
        page = f.read()
    return {"text": page}


def test_obtain_schema_and_push():
    test_url = (
        "https://www.funda.nl/koop/rotterdam/appartement-43494363-nobelstraat-37-c/"
    )
    raw_page_content = get_raw_page_content()
    llm_response = get_llm_response()
    db = DatabaseClient(load_config())
    # client = MagicMock()
    # client.invoke = MagicMock(return_value=llm_response)
    client = OpenAI()

    obtain_schema_and_push(raw_page_content, test_url, client, db)
