import pytest
from mock import MagicMock
from fundai.scraper import get_links_from_page, get_max_page


@pytest.fixture
def search_page() -> str:
    with open("./tests/raw_search_page.txt", "r") as f:
        page = f.read().replace("\\n", "\n")
    return page


actual_house_urls = {
    "https://www.funda.nl/koop/rotterdam/appartement-43503188-rietdekkerweg-63/",
    "https://www.funda.nl/koop/rotterdam/appartement-43503798-van-cittersstraat-30-d/",
    "https://www.funda.nl/koop/rotterdam/appartement-43503528-godijn-van-dormaalstraat-140/",
    "https://www.funda.nl/koop/rotterdam/huis-43503407-mecklenburglaan-30/",
    "https://www.funda.nl/koop/rotterdam/huis-43502338-driedistel-38/",
    "https://www.funda.nl/koop/rotterdam/appartement-43502006-heindijk-294/",
    "https://www.funda.nl/koop/rotterdam/appartement-43502764-stationssingel-13-c/",
    "https://www.funda.nl/koop/rotterdam/appartement-43502534-corsicalaan-119/",
    "https://www.funda.nl/koop/rotterdam/appartement-43502415-statensingel-40-a/",
    "https://www.funda.nl/koop/rotterdam/appartement-43501363-weena-1155/",
    "https://www.funda.nl/koop/rotterdam/appartement-43501659-van-der-meydestraat-13-a/",
    "https://www.funda.nl/koop/rotterdam/appartement-43501508-prinsenlaan-631-a/",
    "https://www.funda.nl/koop/rotterdam/appartement-43501411-everaertstraat-95/",
    "https://www.funda.nl/koop/rotterdam/appartement-43500174-peppelweg-60-c/",
    "https://www.funda.nl/koop/rotterdam/huis-43500065-helsinkipad-5/",
}


def test_get_links_from_pages(search_page: str):
    home_type = "koop"
    area = "rotterdam"
    urls = get_links_from_page(search_page, home_type, area)
    print(urls)
    assert urls == actual_house_urls


def test_get_max_page(search_page: str):
    m = get_max_page(search_page)
    assert m == 190
