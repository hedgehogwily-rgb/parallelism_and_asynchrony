import asyncio
import logging

from crawler import AsyncCrawler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)

async def main() -> None:
    async with AsyncCrawler(max_concurrent=10, max_depth=2, total_timeout=10) as crawler:
        results = await crawler.crawl(
            start_urls=["https://example.com"],
            max_pages=50,
            same_domain_only=True,
        )

    print("=" * 60)
    print(f"Processed pages: {len(results)}")
    print(f"Failed pages:    {len(crawler.failed_urls)}")
    print(f"Visited pages:   {len(crawler.visited_urls)}")
    print("=" * 60)

    for url, page in list(results.items())[:5]:
        print(f"URL: {url}")
        print(f"  Title: {page.get('title', '')}")
        print(f"  Text length: {len(page.get('text', ''))}")
        print(f"  Links: {len(page.get('links', []))}")
        print("-" * 60)


if __name__ == "__main__":
    asyncio.run(main())
