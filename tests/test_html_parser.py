import pytest

from HTMLParser import HTMLParser

VALID_HTML = """
<html>
  <head>
    <title>  Test Page  </title>
    <meta name="description" content="A test page">
    <meta name="keywords" content="test, html, parser">
  </head>
  <body>
    <h1>Main</h1>
    <h2>Section</h2>
    <p>Hello <b>world</b></p>
    <a href="/about">About</a>
    <a href="https://other.com/page">External</a>
    <a href="mailto:test@example.com">Mail</a>
    <a href="#anchor">Anchor</a>
    <img src="/logo.png" alt="Logo">
    <img src="/no-alt.png">
    <table>
      <tr><th>Name</th><th>Age</th></tr>
      <tr><td>Oleg</td><td>25</td></tr>
    </table>
    <ul>
      <li>First</li>
      <li>Second</li>
    </ul>
  </body>
</html>
"""

BASE_URL = "https://example.com/dir/page.html"


@pytest.mark.asyncio
async def test_parse_valid_html():
    parser = HTMLParser()
    result = await parser.parse_html(VALID_HTML, BASE_URL)
    assert result["url"] == BASE_URL
    assert result["title"] == "Test Page"
    assert "Hello" in result["text"]
    assert result["metadata"]["description"] == "A test page"
    assert result["metadata"]["keywords"] == "test, html, parser"


@pytest.mark.asyncio
async def test_broken_html_does_not_crash():
    parser = HTMLParser()
    result = await parser.parse_html("<html><body><p>oops", BASE_URL)
    assert result["url"] == BASE_URL
    assert "text" in result
    assert isinstance(result["links"], list)


@pytest.mark.asyncio
async def test_extract_links_filters_and_absolutizes():
    parser = HTMLParser()
    result = await parser.parse_html(VALID_HTML, BASE_URL)
    links = result["links"]
    assert "https://example.com/about" in links
    assert "https://other.com/page" in links
    assert all(link.startswith("http") for link in links)
    assert not any(link.startswith("mailto:") for link in links)


def test_relative_url_conversion():
    from bs4 import BeautifulSoup

    parser = HTMLParser()
    soup = BeautifulSoup('<a href="page2.html">x</a>', "lxml")
    links = parser.extract_links(soup, "https://example.com/dir/")
    assert links == ["https://example.com/dir/page2.html"]


@pytest.mark.asyncio
async def test_extract_images_headings_tables_lists():
    parser = HTMLParser()
    result = await parser.parse_html(VALID_HTML, BASE_URL)

    assert {"src": "https://example.com/logo.png", "alt": "Logo"} in result["images"]
    assert {"src": "https://example.com/no-alt.png", "alt": ""} in result["images"]

    assert {"level": "h1", "text": "Main"} in result["headings"]
    assert {"level": "h2", "text": "Section"} in result["headings"]

    assert result["tables"][0]["rows"] == [["Name", "Age"], ["Oleg", "25"]]

    assert result["lists"][0]["type"] == "ul"
    assert result["lists"][0]["items"] == ["First", "Second"]


@pytest.mark.asyncio
async def test_fetch_and_parse_integration():
    from crawler import AsyncCrawler

    async with AsyncCrawler(max_concurrent=2, total_timeout=10) as crawler:
        result = await crawler.fetch_and_parse("https://example.com")
    assert result["url"] == "https://example.com"
    assert result["title"]
    assert isinstance(result["links"], list)
