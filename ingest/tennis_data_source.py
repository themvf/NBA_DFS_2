"""Discover yearly workbooks from the provider's index once per process."""
from functools import lru_cache
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

import requests

INDEX_URL = "https://www.tennis-data.co.uk/alldata.php"


class _Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() == "a":
            self.links.extend(value for key, value in attrs if key.lower() == "href" and value)


@lru_cache(maxsize=1)
def _workbook_links():
    response = requests.get(INDEX_URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=40)
    response.raise_for_status()
    parser = _Links()
    parser.feed(response.text)
    return tuple(urljoin(INDEX_URL, link) for link in parser.links
                 if urlparse(urljoin(INDEX_URL, link)).hostname == "www.tennis-data.co.uk"
                 and urlparse(link).path.lower().endswith(".xlsx"))


def workbook_url(tour: str, year: int) -> str:
    if tour not in ("ATP", "WTA"):
        raise ValueError(f"Unsupported tennis tour: {tour!r}")
    suffix = f"/{int(year)}{'w' if tour == 'WTA' else ''}/{int(year)}.xlsx"
    links = _workbook_links()
    matches = sorted({link for link in links if urlparse(link).path.endswith(suffix)})
    if len(matches) != 1:
        raise ValueError(f"Tennis workbook discovery at {INDEX_URL}: expected exactly one *{suffix}; "
                         f"matches={matches!r}; discovered={list(links)!r}")
    return matches[0]
