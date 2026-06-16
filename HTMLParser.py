from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import logging

logger = logging.getLogger(__name__)

class HTMLParser:
    @staticmethod
    def _empty_result(url: str) -> dict:
        return {
            'url': url,
            'title': "",
            'text': "",
            'links': [],
            'metadata': {},
            'images': [],
            'headings': [],
            'tables': [],
            'lists': [],
        }

    @staticmethod
    def _get_title(soup: BeautifulSoup) -> str:
        if soup.title and soup.title.string:
            return soup.title.string.strip()
        return ""

    async def parse_html(self, html: str, url: str) -> dict:
        logger.info("Parsing HTML for %s", url)
        result = self._empty_result(url)
        try:
            soup = BeautifulSoup(html, 'lxml')
            result.update({
                'title': self._get_title(soup),
                'text': self.extract_text(soup),
                'links': self.extract_links(soup, url),
                'metadata': self.extract_metadata(soup),
                'images': self.extract_images(soup, url),
                'headings': self.extract_headings(soup),
                'tables': self.extract_tables(soup),
                'lists': self.extract_lists(soup),
            })
        except Exception as e:
            logger.warning("Error parsing HTML for %s: %s", url, e)
            result['error'] = str(e)
        return result

    def extract_links(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        links = []
        for link in soup.find_all('a'):
            href = link.get('href')
            if href:
                absolute_url = urljoin(base_url, href)
                parsed = urlparse(absolute_url)
                if parsed.scheme in {"http", "https"}:
                    links.append(absolute_url)
        return links

    def extract_text(self, soup: BeautifulSoup, selector: str = None) -> str:
        if selector:
            node = soup.select_one(selector)
            return node.get_text(separator=" ", strip=True) if node else ""
        return soup.get_text(separator=" ", strip=True)


    def extract_metadata(self, soup: BeautifulSoup) -> dict:
        description_data = soup.find("meta", attrs={"name": "description"})
        description = description_data.get('content') if description_data else ""
        keywords_data = soup.find("meta", attrs={"name": "keywords"})
        keywords = keywords_data.get('content') if keywords_data else ""
        return {
            'title': self._get_title(soup),
            'description': description,
            'keywords': keywords,
        }

    def extract_images(self, soup: BeautifulSoup, base_url: str) -> list[dict]:
        images = []
        for img in soup.find_all('img'):
            src = img.get('src')
            if not src:
                continue
            images.append({
                'src': urljoin(base_url, src),
                'alt': img.get('alt', ""),
            })
        return images

    def extract_headings(self, soup: BeautifulSoup) -> list[dict]:
        headings = []
        for heading in soup.find_all(['h1', 'h2', 'h3']):
            level = heading.name
            text = heading.get_text(strip=True)
            headings.append({
                'level': level,
                'text': text,
            })
        return headings

    def extract_tables(self, soup: BeautifulSoup) -> list[dict]:
        tables = []
        for table in soup.find_all('table'):
            rows = []
            for row in table.find_all('tr'):
                cells = [
                    cell.get_text(strip=True)
                    for cell in row.find_all(['td', 'th'])
                ]
                rows.append(cells)
            tables.append({
                'rows': rows,
            })
        return tables

    def extract_lists(self, soup: BeautifulSoup) -> list[dict]:
        lists = []
        for list_tag in soup.find_all(['ul', 'ol']):
            items = [item.get_text(strip=True) for item in list_tag.find_all('li')]
            lists.append({
                'items': items,
                'type': list_tag.name,
            })
        return lists