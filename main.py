import asyncio
import logging
import time

from crawler import AsyncCrawler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)

URLS = [
    "https://example.com",
    "https://httpbin.org/get",
    "https://httpbin.org/delay/1",
    "https://httpbin.org/delay/2",
    "https://httpbin.org/status/404",
    "https://this-domain-does-not-exist-xyz.test",
    "https://httpbin.org/delay/10",
]


def preview(value: str, limit: int = 80) -> str:
    text = value.replace("\n", " ")
    return text if len(text) <= limit else text[: limit - 3] + "..."


async def main() -> None:
    async with AsyncCrawler(max_concurrent=5, total_timeout=5) as crawler:
        logger.info("Начало последовательной загрузки")
        t0 = time.perf_counter()
        seq_status: dict[str, str] = {}
        for url in URLS:
            try:
                body = await crawler.fetch_url(url)
                seq_status[url] = f"OK ({len(body)} bytes)"
            except Exception as exc:
                seq_status[url] = f"ERROR: {type(exc).__name__}"
        seq_time = time.perf_counter() - t0

        logger.info("Начало параллельной загрузки")
        t0 = time.perf_counter()
        results = await crawler.fetch_urls(URLS)
        par_time = time.perf_counter() - t0

    logger.info("Результаты параллельной загрузки")
    for url, result in results.items():
        logger.info(f"{url} -> {preview(result)}")

    logger.info("Статус последовательной загрузки")
    for url, status in seq_status.items():
        logger.info(f"{url} -> {status}")

    speedup = seq_time / par_time if par_time > 0 else float("inf")
    logger.info(
        f"\nsequential={seq_time:.2f}s  parallel={par_time:.2f}s  "
        f"speedup={speedup:.2f}x"
    )
    logger.info(f"Loaded {sum(1 for r in results.values() if not r.startswith('ERROR:'))} pages")


if __name__ == "__main__":
    asyncio.run(main())
