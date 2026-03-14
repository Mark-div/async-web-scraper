import asyncio
import logging
import random
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


@dataclass
class ScrapedItem:
    url: str
    title: str
    content: str
    metadata: dict = field(default_factory=dict)
    scraped_at: datetime = field(default_factory=datetime.utcnow)
    status: str = "success"


@dataclass
class ScraperConfig:
    max_retries: int = 3
    retry_delay: float = 2.0
    request_timeout: int = 30
    max_concurrent: int = 10
    rate_limit_delay: float = 1.0
    user_agents: list = field(default_factory=lambda: [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    ])
    proxies: list = field(default_factory=list)


class AsyncScraper:
    def __init__(self, config: Optional[ScraperConfig] = None):
        self.config = config or ScraperConfig()
        self._semaphore = asyncio.Semaphore(self.config.max_concurrent)
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.config.request_timeout),
            connector=aiohttp.TCPConnector(ssl=False),
        )
        return self

    async def __aexit__(self, *args):
        if self._session:
            await self._session.close()

    def _get_headers(self) -> dict:
        return {
            "User-Agent": random.choice(self.config.user_agents),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
        }

    def _get_proxy(self) -> Optional[str]:
        if not self.config.proxies:
            return None
        return random.choice(self.config.proxies)

    async def fetch(self, url: str, attempt: int = 0) -> Optional[str]:
        async with self._semaphore:
            try:
                await asyncio.sleep(self.config.rate_limit_delay)
                proxy = self._get_proxy()
                async with self._session.get(
                    url,
                    headers=self._get_headers(),
                    proxy=proxy,
                    allow_redirects=True,
                ) as response:
                    response.raise_for_status()
                    return await response.text()

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt < self.config.max_retries:
                    delay = self.config.retry_delay * (2 ** attempt)
                    logger.warning(f"Retry {attempt + 1}/{self.config.max_retries} for {url}: {e}")
                    await asyncio.sleep(delay)
                    return await self.fetch(url, attempt + 1)
                logger.error(f"Failed to fetch {url} after {self.config.max_retries} retries")
                return None

    def parse(self, html: str, url: str) -> ScrapedItem:
        soup = BeautifulSoup(html, "lxml")

        title = ""
        if soup.title:
            title = soup.title.string or ""
        elif soup.find("h1"):
            title = soup.find("h1").get_text(strip=True)

        # Remove noise
        for tag in soup(["script", "style", "nav", "footer", "iframe"]):
            tag.decompose()

        content = soup.get_text(separator=" ", strip=True)
        content = " ".join(content.split())  # Normalize whitespace

        metadata = {
            "domain": urlparse(url).netloc,
            "links_count": len(soup.find_all("a", href=True)),
            "images_count": len(soup.find_all("img")),
            "word_count": len(content.split()),
        }

        return ScrapedItem(url=url, title=title, content=content, metadata=metadata)

    async def scrape_url(self, url: str) -> Optional[ScrapedItem]:
        html = await self.fetch(url)
        if not html:
            return ScrapedItem(url=url, title="", content="", status="failed")
        return self.parse(html, url)

    async def scrape_many(self, urls: list[str]) -> list[ScrapedItem]:
        tasks = [self.scrape_url(url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        items = []
        for url, result in zip(urls, results):
            if isinstance(result, Exception):
                logger.error(f"Exception scraping {url}: {result}")
                items.append(ScrapedItem(url=url, title="", content="", status="error"))
            elif result:
                items.append(result)
        return items
