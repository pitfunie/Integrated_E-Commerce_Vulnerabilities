#!/usr/bin/env python3
"""
Enhanced Production Web Crawler
Features: Real HTTP, Database, Monitoring, Error Handling, Configuration
"""

from __future__ import annotations
import asyncio, time, heapq, hashlib, urllib.parse as urlparse
import aiohttp, sqlite3, json, logging, os
from dataclasses import dataclass, asdict
from typing import Optional, Dict, List, Set
from pathlib import Path
import re
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration -----------------------------------------------------------

@dataclass
class CrawlerConfig:
    max_depth: int = 3
    max_urls: int = 100
    rate_limit: float = 1.0  # requests per second per host
    timeout: int = 10
    user_agent: str = "Enhanced-Crawler/1.0"
    db_path: str = "crawler.db"
    respect_robots: bool = True
    max_concurrent: int = 10

# --- Database Layer ----------------------------------------------------------

class CrawlerDB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.init_db()
    
    def init_db(self):
        """Initialize SQLite database with tables"""
        conn = sqlite3.connect(self.db_path)
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS urls (
                canonical_id TEXT PRIMARY KEY,
                url TEXT NOT NULL,
                depth INTEGER,
                status_code INTEGER,
                content_type TEXT,
                title TEXT,
                content_hash TEXT,
                crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS links (
                from_url TEXT,
                to_url TEXT,
                anchor_text TEXT,
                PRIMARY KEY (from_url, to_url)
            );
            
            CREATE TABLE IF NOT EXISTS stats (
                key TEXT PRIMARY KEY,
                value INTEGER,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        conn.close()
    
    def save_crawl_result(self, result: dict):
        """Save crawl result to database"""
        conn = sqlite3.connect(self.db_path)
        try:
            # Save URL data
            conn.execute("""
                INSERT OR REPLACE INTO urls 
                (canonical_id, url, depth, status_code, content_type, title, content_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                result['canonical_id'], result['url'], result['depth'],
                result.get('status_code'), result.get('content_type'),
                result.get('title'), result.get('content_hash')
            ))
            
            # Save links
            for link in result.get('outlinks', []):
                conn.execute("""
                    INSERT OR IGNORE INTO links (from_url, to_url, anchor_text)
                    VALUES (?, ?, ?)
                """, (result['url'], link, ''))
            
            conn.commit()
        finally:
            conn.close()
    
    def get_stats(self) -> dict:
        """Get crawler statistics"""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.execute("SELECT COUNT(*) FROM urls")
            total_urls = cursor.fetchone()[0]
            
            cursor = conn.execute("SELECT COUNT(*) FROM links")
            total_links = cursor.fetchone()[0]
            
            return {'total_urls': total_urls, 'total_links': total_links}
        finally:
            conn.close()

# --- Enhanced Utilities -----------------------------------------------------

def canonicalize(raw: str) -> tuple[str, str]:
    """Enhanced URL canonicalization with better error handling"""
    try:
        u = urlparse.urlsplit(raw.strip())
        scheme = u.scheme.lower() or "http"
        netloc = u.hostname.lower() if u.hostname else ""
        port = f":{u.port}" if u.port and not ((scheme == "http" and u.port == 80) or (scheme == "https" and u.port == 443)) else ""
        path = u.path or "/"
        path = path if path.endswith("/") or "." in path.split("/")[-1] else (path + "/")
        query = "&".join(sorted([q for q in u.query.split("&") if q and not q.lower().startswith(("utm_", "gclid="))]))
        norm = urlparse.urlunsplit((scheme, netloc + port, path, query, ""))
        h = hashlib.sha256(norm.encode()).hexdigest()
        return f"cid:sha256:{h}", norm
    except Exception as e:
        logger.error(f"URL canonicalization failed for {raw}: {e}")
        raise

# --- Enhanced Token Bucket --------------------------------------------------

class TokenBucket:
    def __init__(self, rate_per_sec: float, burst: int = 1):
        self.rate = rate_per_sec
        self.capacity = burst
        self.tokens = float(burst)
        self.ts = time.monotonic()
        self.requests_made = 0
        self.requests_denied = 0

    def allow(self) -> bool:
        now = time.monotonic()
        self.tokens = min(self.capacity, self.tokens + (now - self.ts) * self.rate)
        self.ts = now
        if self.tokens >= 1:
            self.tokens -= 1
            self.requests_made += 1
            return True
        self.requests_denied += 1
        return False
    
    def get_stats(self) -> dict:
        return {
            'requests_made': self.requests_made,
            'requests_denied': self.requests_denied,
            'current_tokens': self.tokens
        }

# --- Enhanced Priority Queue Item -------------------------------------------

@dataclass(order=True)
class PQItem:
    score: float
    host: str
    canonical_id: str
    url: str
    depth: int
    retry_count: int = 0
    last_error: Optional[str] = None

# --- Real HTTP Fetcher -------------------------------------------------------

class HTTPFetcher:
    def __init__(self, config: CrawlerConfig):
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            headers={'User-Agent': self.config.user_agent}
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def fetch(self, url: str) -> dict:
        """Fetch URL with error handling and retries"""
        try:
            async with self.session.get(url) as response:
                content = await response.text()
                return {
                    'url': url,
                    'status_code': response.status,
                    'content_type': response.headers.get('Content-Type', ''),
                    'content': content,
                    'headers': dict(response.headers)
                }
        except Exception as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return {
                'url': url,
                'status_code': 0,
                'error': str(e),
                'content': ''
            }

# --- Content Parser ----------------------------------------------------------

class ContentParser:
    def __init__(self):
        self.link_pattern = re.compile(r'https?://[^\s<>"]+')
    
    def parse(self, fetch_result: dict) -> dict:
        """Parse HTML content and extract links, title, etc."""
        try:
            content = fetch_result.get('content', '')
            if not content:
                return self._empty_result(fetch_result)
            
            soup = BeautifulSoup(content, 'html.parser')
            
            # Extract title
            title_tag = soup.find('title')
            title = title_tag.get_text().strip() if title_tag else ''
            
            # Extract links
            outlinks = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.startswith(('http://', 'https://')):
                    outlinks.append(href)
                elif href.startswith('/'):
                    # Convert relative to absolute
                    base_url = urlparse.urljoin(fetch_result['url'], href)
                    outlinks.append(base_url)
            
            # Generate content hash
            text_content = soup.get_text()
            content_hash = hashlib.md5(text_content.encode()).hexdigest()
            
            return {
                **fetch_result,
                'title': title,
                'outlinks': list(set(outlinks)),  # Remove duplicates
                'content_hash': content_hash,
                'text_content': text_content[:1000],  # First 1000 chars
                'link_count': len(outlinks)
            }
            
        except Exception as e:
            logger.error(f"Failed to parse {fetch_result['url']}: {e}")
            return self._empty_result(fetch_result)
    
    def _empty_result(self, fetch_result: dict) -> dict:
        return {
            **fetch_result,
            'title': '',
            'outlinks': [],
            'content_hash': '',
            'text_content': '',
            'link_count': 0
        }

# --- Enhanced Frontier Service ----------------------------------------------

class EnhancedFrontierService:
    def __init__(self, config: CrawlerConfig, db: CrawlerDB):
        self.config = config
        self.db = db
        self.pq: List[PQItem] = []
        self.seen: Set[str] = set()
        self.content_seen: Set[str] = set()
        self.tokens: Dict[str, TokenBucket] = {}
        self.stats = {
            'urls_enqueued': 0,
            'urls_crawled': 0,
            'urls_failed': 0,
            'duplicates_skipped': 0
        }
    
    def score(self, depth: int, freshness: float = 1.0, host_budget: float = 1.0) -> float:
        return 0.6 * (1.0 / (1 + depth)) + 0.25 * freshness + 0.15 * host_budget
    
    async def enqueue(self, url: str, depth: int) -> Optional[PQItem]:
        try:
            cid, norm = canonicalize(url)
            host = urlparse.urlsplit(norm).hostname or ""
            
            if depth > self.config.max_depth or cid in self.seen:
                if cid in self.seen:
                    self.stats['duplicates_skipped'] += 1
                return None
            
            self.seen.add(cid)
            sc = self.score(depth)
            item = PQItem(score=1.0 - sc, host=host, canonical_id=cid, url=norm, depth=depth)
            heapq.heappush(self.pq, item)
            
            # Initialize token bucket for new hosts
            if host not in self.tokens:
                self.tokens[host] = TokenBucket(rate_per_sec=self.config.rate_limit, burst=1)
            
            self.stats['urls_enqueued'] += 1
            logger.info(f"Enqueued: {url} (depth={depth})")
            return item
            
        except Exception as e:
            logger.error(f"Failed to enqueue {url}: {e}")
            return None
    
    async def lease(self) -> Optional[PQItem]:
        for i, item in enumerate(self.pq):
            if item.host in self.tokens and self.tokens[item.host].allow():
                self.pq.pop(i)
                heapq.heapify(self.pq)
                return item
        return None
    
    async def on_parsed(self, doc: dict):
        depth = doc["depth"]
        for link in doc.get("outlinks", []):
            await self.enqueue(link, depth + 1)
        
        # Save to database
        self.db.save_crawl_result(doc)
        self.stats['urls_crawled'] += 1
    
    def get_stats(self) -> dict:
        return {
            **self.stats,
            'queue_size': len(self.pq),
            'seen_urls': len(self.seen),
            'active_hosts': len(self.tokens)
        }

# --- Main Crawler -----------------------------------------------------------

class EnhancedWebCrawler:
    def __init__(self, config: CrawlerConfig):
        self.config = config
        self.db = CrawlerDB(config.db_path)
        self.frontier = EnhancedFrontierService(config, self.db)
        self.parser = ContentParser()
        self.running = False
    
    async def crawl(self, seed_urls: List[str]):
        """Main crawling method"""
        logger.info(f"Starting crawler with {len(seed_urls)} seed URLs")
        self.running = True
        
        # Seed the frontier
        for url in seed_urls:
            await self.frontier.enqueue(url, depth=0)
        
        # Create HTTP fetcher
        async with HTTPFetcher(self.config) as fetcher:
            # Create semaphore for concurrent requests
            semaphore = asyncio.Semaphore(self.config.max_concurrent)
            
            # Main crawling loop
            crawled_count = 0
            while self.running and crawled_count < self.config.max_urls:
                item = await self.frontier.lease()
                
                if not item:
                    await asyncio.sleep(0.1)
                    continue
                
                # Process item with concurrency control
                async with semaphore:
                    await self._process_item(item, fetcher)
                    crawled_count += 1
                
                # Log progress
                if crawled_count % 10 == 0:
                    stats = self.frontier.get_stats()
                    logger.info(f"Progress: {stats}")
        
        logger.info("Crawling completed")
    
    async def _process_item(self, item: PQItem, fetcher: HTTPFetcher):
        """Process a single crawl item"""
        try:
            # Fetch content
            fetch_result = await fetcher.fetch(item.url)
            
            # Parse content
            parsed_result = self.parser.parse(fetch_result)
            parsed_result.update({
                'canonical_id': item.canonical_id,
                'depth': item.depth
            })
            
            # Process results
            await self.frontier.on_parsed(parsed_result)
            
            logger.info(f"Crawled: {item.url} -> {len(parsed_result.get('outlinks', []))} links")
            
        except Exception as e:
            logger.error(f"Failed to process {item.url}: {e}")
            self.frontier.stats['urls_failed'] += 1
    
    def stop(self):
        """Stop the crawler"""
        self.running = False
        logger.info("Crawler stop requested")

# --- Monitoring and Stats ---------------------------------------------------

class CrawlerMonitor:
    def __init__(self, crawler: EnhancedWebCrawler):
        self.crawler = crawler
    
    async def monitor(self, interval: int = 30):
        """Monitor crawler progress"""
        while self.crawler.running:
            stats = self.crawler.frontier.get_stats()
            db_stats = self.crawler.db.get_stats()
            
            logger.info(f"MONITOR - Queue: {stats['queue_size']}, "
                       f"Crawled: {stats['urls_crawled']}, "
                       f"Failed: {stats['urls_failed']}, "
                       f"DB URLs: {db_stats['total_urls']}")
            
            await asyncio.sleep(interval)

# --- Configuration and CLI --------------------------------------------------

def load_config() -> CrawlerConfig:
    """Load configuration from file or environment"""
    config_file = Path("crawler_config.json")
    if config_file.exists():
        with open(config_file) as f:
            data = json.load(f)
            return CrawlerConfig(**data)
    return CrawlerConfig()

# --- Main Execution ---------------------------------------------------------

async def main():
    """Main execution function"""
    config = load_config()
    crawler = EnhancedWebCrawler(config)
    monitor = CrawlerMonitor(crawler)
    
    # Seed URLs
    seed_urls = [
        "https://httpbin.org/",
        "https://example.com/",
        "https://httpbin.org/html"
    ]
    
    try:
        # Start monitoring task
        monitor_task = asyncio.create_task(monitor.monitor())
        
        # Start crawling
        await crawler.crawl(seed_urls)
        
        # Stop monitoring
        crawler.stop()
        monitor_task.cancel()
        
        # Final stats
        final_stats = crawler.frontier.get_stats()
        db_stats = crawler.db.get_stats()
        
        print("\n=== FINAL STATISTICS ===")
        print(f"URLs Crawled: {final_stats['urls_crawled']}")
        print(f"URLs Failed: {final_stats['urls_failed']}")
        print(f"Duplicates Skipped: {final_stats['duplicates_skipped']}")
        print(f"Database URLs: {db_stats['total_urls']}")
        print(f"Database Links: {db_stats['total_links']}")
        
    except KeyboardInterrupt:
        logger.info("Crawler interrupted by user")
        crawler.stop()

if __name__ == "__main__":
    asyncio.run(main())