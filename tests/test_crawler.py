import asyncio
import time

import aiohttp
import pytest

from crawler import AsyncCrawler


@pytest.mark.asyncio
async def test_fetch_valid_url():
    async with AsyncCrawler(max_concurrent=2, total_timeout=10) as crawler:
        text = await crawler.fetch_url("https://httpbin.org/get")
    assert text
    assert '"url"' in text


@pytest.mark.asyncio
async def test_fetch_404_raises():
    async with AsyncCrawler(max_concurrent=2, total_timeout=10) as crawler:
        with pytest.raises(aiohttp.ClientResponseError):
            await crawler.fetch_url("https://httpbin.org/status/404")


@pytest.mark.asyncio
async def test_fetch_urls_handles_404():
    async with AsyncCrawler(max_concurrent=2, total_timeout=10) as crawler:
        results = await crawler.fetch_urls(["https://httpbin.org/status/404"])
    assert "ERROR:" in results["https://httpbin.org/status/404"]
    assert "ClientResponseError" in results["https://httpbin.org/status/404"]


@pytest.mark.asyncio
async def test_timeout():
    async with AsyncCrawler(max_concurrent=2, total_timeout=1) as crawler:
        with pytest.raises(asyncio.TimeoutError):
            await crawler.fetch_url("https://httpbin.org/delay/3")


@pytest.mark.asyncio
async def test_parallel_faster_than_sequential():
    urls = [
        "https://httpbin.org/delay/1",
        "https://httpbin.org/delay/1",
    ]
    async with AsyncCrawler(max_concurrent=2, total_timeout=15) as crawler:
        t0 = time.perf_counter()
        for url in urls:
            await crawler.fetch_url(url)
        seq_time = time.perf_counter() - t0

        t0 = time.perf_counter()
        await crawler.fetch_urls(urls)
        par_time = time.perf_counter() - t0

    assert par_time < seq_time * 0.7


@pytest.mark.asyncio
async def test_close_idempotent():
    crawler = AsyncCrawler(max_concurrent=2, total_timeout=10)
    await crawler.fetch_url("https://httpbin.org/get")
    await crawler.close()
    await crawler.close()
    assert crawler._session is None or crawler._session.closed

    async with AsyncCrawler(max_concurrent=2, total_timeout=10) as c:
        await c.fetch_url("https://httpbin.org/get")
    assert c._session is None or c._session.closed
