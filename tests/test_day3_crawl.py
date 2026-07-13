import asyncio
import time

import pytest

from crawler import AsyncCrawler, CrawlerQueue


@pytest.mark.asyncio
async def test_queue_priority():
    queue = CrawlerQueue()
    queue.add_url("https://example.com/low", priority=10, depth=0)
    queue.add_url("https://example.com/high", priority=1, depth=0)

    first = await queue.get_next()
    assert first == "https://example.com/high"
    assert queue.pop_depth(first) == 0


@pytest.mark.asyncio
async def test_queue_get_next_returns_str_or_none():
    queue = CrawlerQueue()
    assert await queue.get_next() is None

    queue.add_url("https://example.com/a", priority=0, depth=2)
    url = await queue.get_next()
    assert isinstance(url, str)
    assert url == "https://example.com/a"
    assert queue.pop_depth(url) == 2


@pytest.mark.asyncio
async def test_crawl_multi_domain_start_urls(monkeypatch):
    async def fake_fetch_and_parse(self, url: str):
        return {
            "url": url,
            "title": "t",
            "text": "ok",
            "links": [],
            "metadata": {},
            "images": [],
            "headings": [],
            "tables": [],
            "lists": [],
        }

    monkeypatch.setattr(AsyncCrawler, "fetch_and_parse", fake_fetch_and_parse)

    crawler = AsyncCrawler(max_concurrent=2, max_depth=1)
    results = await crawler.crawl(
        start_urls=[
            "https://example.com/root",
            "https://other.com/root",
        ],
        same_domain_only=True,
    )

    assert "https://example.com/root" in results
    assert "https://other.com/root" in results


@pytest.mark.asyncio
async def test_crawl_runs_in_parallel(monkeypatch):
    urls = [
        "https://example.com/a",
        "https://example.com/b",
        "https://example.com/c",
    ]
    active = 0
    max_active = 0

    async def fake_fetch_and_parse(self, url: str):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.2)
        active -= 1
        return {
            "url": url,
            "title": "t",
            "text": "ok",
            "links": [],
            "metadata": {},
            "images": [],
            "headings": [],
            "tables": [],
            "lists": [],
        }

    monkeypatch.setattr(AsyncCrawler, "fetch_and_parse", fake_fetch_and_parse)

    crawler = AsyncCrawler(max_concurrent=3, max_depth=0)
    t0 = time.perf_counter()
    results = await crawler.crawl(
        start_urls=urls,
        max_pages=3,
        same_domain_only=True,
    )
    elapsed = time.perf_counter() - t0

    assert len(results) == 3
    assert max_active > 1
    assert elapsed < 0.5


@pytest.mark.asyncio
async def test_crawl_max_depth_limit(monkeypatch):
    graph = {
        "https://example.com/root": ["https://example.com/a"],
        "https://example.com/a": ["https://example.com/b"],
        "https://example.com/b": [],
    }

    async def fake_fetch_and_parse(self, url: str):
        return {
            "url": url,
            "title": url.rsplit("/", 1)[-1],
            "text": "ok",
            "links": graph.get(url, []),
            "metadata": {},
            "images": [],
            "headings": [],
            "tables": [],
            "lists": [],
        }

    monkeypatch.setattr(AsyncCrawler, "fetch_and_parse", fake_fetch_and_parse)

    crawler = AsyncCrawler(max_concurrent=2, max_depth=1)
    results = await crawler.crawl(
        start_urls=["https://example.com/root"],
        max_pages=10,
        max_depth=1,
        same_domain_only=True,
    )

    assert "https://example.com/root" in results
    assert "https://example.com/a" in results
    assert "https://example.com/b" not in results


@pytest.mark.asyncio
async def test_crawl_url_filtering(monkeypatch):
    graph = {
        "https://example.com/root": [
            "https://example.com/docs/page",
            "https://example.com/private/secret",
            "https://other.com/docs/page",
        ],
        "https://example.com/docs/page": [],
    }

    async def fake_fetch_and_parse(self, url: str):
        return {
            "url": url,
            "title": "t",
            "text": "ok",
            "links": graph.get(url, []),
            "metadata": {},
            "images": [],
            "headings": [],
            "tables": [],
            "lists": [],
        }

    monkeypatch.setattr(AsyncCrawler, "fetch_and_parse", fake_fetch_and_parse)

    crawler = AsyncCrawler(max_concurrent=2, max_depth=2)
    results = await crawler.crawl(
        start_urls=["https://example.com/root"],
        max_pages=20,
        same_domain_only=True,
        include_patterns=[r"/(root|docs)(/|$)"],
        exclude_patterns=[r"/private/"],
    )

    assert "https://example.com/docs/page" in results
    assert "https://example.com/private/secret" not in crawler.visited_urls
    assert "https://other.com/docs/page" not in crawler.visited_urls


@pytest.mark.asyncio
async def test_crawl_no_duplicate_visits(monkeypatch):
    graph = {
        "https://example.com/root": [
            "https://example.com/a",
            "https://example.com/a",
            "https://example.com/a#fragment",
        ],
        "https://example.com/a": [],
    }
    calls: dict[str, int] = {}

    async def fake_fetch_and_parse(self, url: str):
        calls[url] = calls.get(url, 0) + 1
        return {
            "url": url,
            "title": "t",
            "text": "ok",
            "links": graph.get(url, []),
            "metadata": {},
            "images": [],
            "headings": [],
            "tables": [],
            "lists": [],
        }

    monkeypatch.setattr(AsyncCrawler, "fetch_and_parse", fake_fetch_and_parse)

    crawler = AsyncCrawler(max_concurrent=2, max_depth=2)
    await crawler.crawl(
        start_urls=["https://example.com/root"],
        max_pages=20,
        same_domain_only=True,
    )

    assert calls.get("https://example.com/a", 0) == 1
    assert len(crawler.visited_urls) == len(set(crawler.visited_urls))
