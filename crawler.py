import asyncio
import logging
import time
import re
from collections import defaultdict
from contextlib import asynccontextmanager
from heapq import heappush, heappop
from urllib.parse import urlparse, urldefrag
import aiohttp
from HTMLParser import HTMLParser

logger = logging.getLogger(__name__)


class AsyncCrawler:
    def __init__(
        self,
        max_concurrent: int = 10,
        total_timeout: float = 10.0,
        max_depth: int = 2,
        per_domain_limit: int = 3,
    ):
        self.max_concurrent = max_concurrent
        self.max_depth = max_depth
        self._timeout = aiohttp.ClientTimeout(
            total=total_timeout,
            connect=5,
            sock_read=5,
        )
        self._session: aiohttp.ClientSession | None = None

        self.queue = CrawlerQueue()
        self.sem_manager = SemaphoreManager(
            global_limit=max_concurrent,
            per_domain_limit=per_domain_limit,
        )
        self.visited_urls: set[str] = set()
        self.failed_urls: dict[str, str] = {}
        self.processed_urls: dict[str, dict] = {}
        self._in_flight = 0

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
        parser = HTMLParser()
        try:
            async with self.sem_manager.slot(url):
                html = await self.fetch_url(url)
        except Exception as e:
            logger.warning("Failed to fetch %s: %s", url, e)
            result = parser._empty_result(url)
            result["error"] = str(e)
            return result
        return await parser.parse_html(html, url)


    def _normalize_url(self, url: str) -> str:
        clean, _ = urldefrag(url.strip())  # убрать #anchor
        return clean.rstrip("/")

    def _should_include_url(
        self,
        url: str,
        allowed_domains: set[str] | None,
        same_domain_only: bool,
        include_patterns: list[str] | None,
        exclude_patterns: list[str] | None,
    ) -> bool:
        if not url:
            return False
        parsed = urlparse(url)

        if parsed.scheme not in {"http", "https"}:
            return False

        if same_domain_only and allowed_domains is not None:
            if parsed.netloc not in allowed_domains:
                return False

        if exclude_patterns and any(re.search(p, url) for p in exclude_patterns):
            return False

        if include_patterns and not any(re.search(p, url) for p in include_patterns):
            return False

        return True

    async def _worker(
        self,
        effective_depth: int,
        same_domain_only: bool,
        allowed_domains: set[str] | None,
        include_patterns: list[str] | None,
        exclude_patterns: list[str] | None,
        max_pages: int,
        stop_event: asyncio.Event,
        started_at: float,
        state_lock: asyncio.Lock,
    ) -> None:
        while not stop_event.is_set():
            async with state_lock:
                if len(self.processed_urls) >= max_pages:
                    stop_event.set()
                    break

            url = await self.queue.get_next()
            if url is None:
                async with state_lock:
                    if (
                        self.queue.get_stats()["queued"] == 0
                        and self._in_flight == 0
                    ):
                        stop_event.set()
                        break
                await asyncio.sleep(0.05)
                continue

            depth = self.queue.pop_depth(url)

            async with state_lock:
                if url in self.visited_urls or depth > effective_depth:
                    continue

                if len(self.processed_urls) + self._in_flight >= max_pages:
                    stop_event.set()
                    break
                self.visited_urls.add(url)
                self._in_flight += 1

            try:
                result = await self.fetch_and_parse(url)
            except Exception:
                async with state_lock:
                    self._in_flight -= 1
                raise

            async with state_lock:
                self._in_flight -= 1

                if result.get("error"):
                    self.failed_urls[url] = result["error"]
                    self.queue.mark_failed(url, result["error"])
                elif len(self.processed_urls) < max_pages:
                    self.processed_urls[url] = result
                    self.queue.mark_processed(url)

                    if depth < effective_depth:
                        for link in result.get("links", []):
                            nlink = self._normalize_url(link)
                            if (
                                self._should_include_url(
                                    nlink,
                                    allowed_domains,
                                    same_domain_only,
                                    include_patterns,
                                    exclude_patterns,
                                )
                                and nlink not in self.visited_urls
                            ):
                                self.queue.add_url(
                                    nlink, priority=depth + 1, depth=depth + 1
                                )

                if len(self.processed_urls) >= max_pages:
                    stop_event.set()
                    break

                if len(self.processed_urls) % 5 == 0:
                    elapsed = max(0.001, time.perf_counter() - started_at)
                    speed = len(self.processed_urls) / elapsed
                    stats = self.queue.get_stats()
                    logger.info(
                        "Progress: processed=%d queued=%d failed=%d speed=%.2f p/s",
                        len(self.processed_urls),
                        stats["queued"],
                        len(self.failed_urls),
                        speed,
                    )

    async def crawl(
        self,
        start_urls: list[str],
        max_pages: int = 100,
        max_depth: int | None = None,
        same_domain_only: bool = True,
        include_patterns: list[str] | None = None,
        exclude_patterns: list[str] | None = None,
    ) -> dict[str, dict]:
        self.visited_urls.clear()
        self.failed_urls.clear()
        self.processed_urls.clear()
        self._in_flight = 0
        self.queue = CrawlerQueue()

        effective_depth = self.max_depth if max_depth is None else max_depth
        if not start_urls:
            return self.processed_urls

        allowed_domains = {
            urlparse(self._normalize_url(u)).netloc
            for u in start_urls
            if urlparse(u).netloc
        }

        for u in start_urls:
            nu = self._normalize_url(u)
            if self._should_include_url(
                nu, allowed_domains, same_domain_only, include_patterns, exclude_patterns
            ):
                self.queue.add_url(nu, priority=0, depth=0)

        started_at = time.perf_counter()
        stop_event = asyncio.Event()
        state_lock = asyncio.Lock()

        workers = [
            asyncio.create_task(
                self._worker(
                    effective_depth,
                    same_domain_only,
                    allowed_domains,
                    include_patterns,
                    exclude_patterns,
                    max_pages,
                    stop_event,
                    started_at,
                    state_lock,
                )
            )
            for _ in range(self.max_concurrent)
        ]
        await asyncio.gather(*workers)

        elapsed = max(0.001, time.perf_counter() - started_at)
        speed = len(self.processed_urls) / elapsed
        stats = self.queue.get_stats()
        logger.info(
            "Progress: processed=%d queued=%d failed=%d speed=%.2f p/s",
            len(self.processed_urls),
            stats["queued"],
            len(self.failed_urls),
            speed,
        )

        return self.processed_urls


class CrawlerQueue:
    def __init__(self):
        self._heap: list[tuple[int, int, str, int]] = []  # (priority, seq, url, depth)
        self._seq = 0
        self._seen_in_queue: set[str] = set()
        self._processed: set[str] = set()
        self._failed: dict[str, str] = {}
        self._depth_by_url: dict[str, int] = {}

    def add_url(self, url: str, priority: int = 0, depth: int = 0) -> bool:
        if url in self._seen_in_queue or url in self._processed or url in self._failed:
            return False
        heappush(self._heap, (priority, self._seq, url, depth))
        self._seq += 1
        self._seen_in_queue.add(url)
        self._depth_by_url[url] = depth
        return True

    def mark_processed(self, url: str):
        self._processed.add(url)

    def mark_failed(self, url: str, error: str):
        self._failed[url] = error

    async def get_next(self) -> str | None:
        if not self._heap:
            return None
        priority, seq, url, depth = heappop(self._heap)
        self._depth_by_url[url] = depth
        return url

    def pop_depth(self, url: str) -> int:
        return self._depth_by_url.pop(url, 0)
    
    def get_stats(self) -> dict:
        return {
            "queued": len(self._heap),
            "processed_count": len(self._processed),
            "failed_count": len(self._failed),
            "seen_count": len(self._seen_in_queue),
        }


class SemaphoreManager:
    def __init__(self, global_limit: int = 10, per_domain_limit: int = 3):
        self.global_limit = global_limit
        self.per_domain_limit = per_domain_limit

        self._global_sem = asyncio.Semaphore(global_limit)
        self._domain_sems: dict[str, asyncio.Semaphore] = {}

        self.active_total = 0
        self.active_by_domain = defaultdict(int)

    def _get_domain_sem(self, domain: str) -> asyncio.Semaphore:
        if domain not in self._domain_sems:
            self._domain_sems[domain] = asyncio.Semaphore(self.per_domain_limit)
        return self._domain_sems[domain]

    @asynccontextmanager
    async def slot(self, url: str):
        domain = urlparse(url).netloc
        domain_sem = self._get_domain_sem(domain)

        await domain_sem.acquire()
        await self._global_sem.acquire()

        self.active_total += 1
        self.active_by_domain[domain] += 1
        try:
            yield
        finally:
            self.active_total -= 1
            self.active_by_domain[domain] -= 1
            self._global_sem.release()
            domain_sem.release()

    def get_stats(self) -> dict:
        return {
            "global_limit": self.global_limit,
            "active_total": self.active_total,
            "active_by_domain": dict(self.active_by_domain),
        }