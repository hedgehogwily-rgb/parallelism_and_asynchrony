import asyncio
import logging

from crawler import AsyncCrawler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)

URLS = [
    "https://example.com",
    "https://httpbin.org/html",
    "https://httpbin.org/links/5/0",
]


def build_summary(page: dict) -> dict:
    return {
        "url": page.get("url"),
        "title": page.get("title", ""),
        "text_length": len(page.get("text", "")),
        "links_count": len(page.get("links", [])),
        "images_count": len(page.get("images", [])),
        "headings_count": len(page.get("headings", [])),
        "links": page.get("links", [])[:5],
    }


def print_summary(summary: dict) -> None:
    print("=" * 60)
    print(f"URL:       {summary['url']}")
    print(f"Title:     {summary['title']}")
    print(f"Text len:  {summary['text_length']}")
    print(f"Links:     {summary['links_count']}")
    print(f"Images:    {summary['images_count']}")
    print(f"Headings:  {summary['headings_count']}")
    if summary["links"]:
        print("First links:")
        for link in summary["links"]:
            print(f"  - {link}")


async def main() -> None:
    async with AsyncCrawler(max_concurrent=5, total_timeout=10) as crawler:
        pages = await asyncio.gather(
            *(crawler.fetch_and_parse(url) for url in URLS),
            return_exceptions=True,
        )

    for url, page in zip(URLS, pages):
        if isinstance(page, Exception):
            logger.warning("Failed to process %s: %s", url, page)
            continue
        if page.get("error"):
            logger.warning("Parsed with error %s: %s", url, page["error"])
        print_summary(build_summary(page))

    print("=" * 60)
    print(f"Processed {len(URLS)} URLs")


if __name__ == "__main__":
    asyncio.run(main())
