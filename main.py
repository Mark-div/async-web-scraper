import asyncio
import logging
import sys
from pathlib import Path

import click

from scraper.core import AsyncScraper, ScraperConfig
from scraper.storage import export_csv, get_stats, init_db, save_many

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/scraper.log"),
    ],
)
logger = logging.getLogger(__name__)

Path("logs").mkdir(exist_ok=True)


@click.group()
def cli():
    """Async web scraper with retry logic and storage."""
    pass


@cli.command()
@click.argument("urls", nargs=-1, required=True)
@click.option("--concurrent", default=10, help="Max concurrent requests")
@click.option("--delay", default=1.0, help="Delay between requests (seconds)")
@click.option("--retries", default=3, help="Max retries per URL")
@click.option("--proxy-file", default=None, help="File with proxy list (one per line)")
def scrape(urls, concurrent, delay, retries, proxy_file):
    """Scrape one or more URLs."""
    proxies = []
    if proxy_file and Path(proxy_file).exists():
        proxies = Path(proxy_file).read_text().strip().splitlines()
        logger.info(f"Loaded {len(proxies)} proxies")

    config = ScraperConfig(
        max_concurrent=concurrent,
        rate_limit_delay=delay,
        max_retries=retries,
        proxies=proxies,
    )

    init_db()

    async def run():
        async with AsyncScraper(config) as scraper:
            logger.info(f"Scraping {len(urls)} URLs...")
            items = await scraper.scrape_many(list(urls))
            saved = save_many(items)
            logger.info(f"Done. Saved {saved} items.")
            return items

    asyncio.run(run())


@cli.command()
@click.option("--url-file", required=True, help="File with URLs to scrape (one per line)")
@click.option("--concurrent", default=10)
@click.option("--delay", default=1.0)
def scrape_file(url_file, concurrent, delay):
    """Scrape URLs from a file."""
    urls = Path(url_file).read_text().strip().splitlines()
    urls = [u.strip() for u in urls if u.strip()]
    logger.info(f"Loaded {len(urls)} URLs from {url_file}")

    config = ScraperConfig(max_concurrent=concurrent, rate_limit_delay=delay)
    init_db()

    async def run():
        async with AsyncScraper(config) as scraper:
            items = await scraper.scrape_many(urls)
            save_many(items)

    asyncio.run(run())


@cli.command()
@click.option("--output", default="data/export.csv")
def export(output):
    """Export scraped data to CSV."""
    path = export_csv(output)
    click.echo(f"Exported to {path}")


@cli.command()
def stats():
    """Show scraping statistics."""
    s = get_stats()
    click.echo(f"Total: {s['total']}")
    click.echo(f"Success: {s['success']} ({s['success_rate']}%)")
    click.echo(f"Failed: {s['failed']}")
    click.echo(f"Last run: {s['last_run']}")


if __name__ == "__main__":
    cli()
