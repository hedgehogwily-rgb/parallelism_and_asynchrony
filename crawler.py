import asyncio
import logging

import aiohttp

from HTMLParser import HTMLParser

logger = logging.getLogger(__name__)


class AsyncCrawler:
    def __init__(self, max_concurrent: int = 10, total_timeout: float = 10.0):
        self.max_concurrent = max_concurrent
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._timeout = aiohttp.ClientTimeout(
            total=total_timeout,
            connect=5,
            sock_read=5,
        )
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(limit=self.max_concurrent)
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=self._timeout,
            )
        return self._session

    async def fetch_url(self, url: str) -> str:
        session = await self._get_session()
        async with self._semaphore:
            logger.info("GET %s", url)
            try:
                async with session.get(url) as resp:
                    resp.raise_for_status()
                    text = await resp.text()
                    logger.info(
                        "OK  %s [%s, %d bytes]",
                        url,
                        resp.status,
                        len(text),
                    )
                    return text
            except asyncio.TimeoutError:
                logger.warning("TIMEOUT %s", url)
                raise
            except aiohttp.ClientResponseError as e:
                logger.warning("HTTP %s %s: %s", e.status, url, e.message)
                raise
            except aiohttp.ClientError as e:
                logger.warning("NET %s: %s", url, e)
                raise

    async def fetch_urls(self, urls: list[str]) -> dict[str, str]:
        results = await asyncio.gather(
            *(self.fetch_url(u) for u in urls),
            return_exceptions=True,
        )
        return {
            url: (
                result
                if not isinstance(result, Exception)
                else f"ERROR: {type(result).__name__}: {result}"
            )
            for url, result in zip(urls, results)
        }

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def __aenter__(self) -> "AsyncCrawler":
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()


    async def fetch_and_parse(self, url: str) -> dict:
        html = await self.fetch_url(url)
        return await HTMLParser().parse_html(html, url)