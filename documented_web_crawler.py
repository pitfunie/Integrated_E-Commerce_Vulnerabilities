#!/usr/bin/env python3
"""
Web Crawler Depth Microservice Architecture

This module demonstrates a production-ready web crawler with microservice architecture
focusing on the Frontier + Depth/Seen services. It implements:

1. URL canonicalization and deduplication
2. Depth-limited crawling with priority scoring
3. Politeness controls (rate limiting per host)
4. Asynchronous processing pipeline
5. Content indexing and archival preparation

Architecture Components:
- Frontier Service: URL queue management with priority scoring
- Depth Service: Controls crawl depth and prevents infinite loops
- Seen Service: Deduplication using canonical URL IDs
- Token Bucket: Rate limiting for respectful crawling

Real-world Usage:
- Search engine indexing (Google, Bing)
- Web archiving (Internet Archive)
- Content monitoring (copyright protection)
- Data mining and research

Author: Web Crawler Architecture Demo
Date: 2024
"""

from __future__ import annotations
import asyncio
import time
import heapq
import hashlib
import urllib.parse as urlparse
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple, Set

# --- URL Canonicalization Utilities -----------------------------------------

def canonicalize(raw_url: str) -> Tuple[str, str]:
    """
    Convert a raw URL into a canonical form for deduplication.
    
    This function normalizes URLs to prevent crawling the same content multiple times
    due to URL variations (different schemes, trailing slashes, query parameters, etc.)
    
    Args:
        raw_url (str): The original URL to canonicalize
        
    Returns:
        Tuple[str, str]: (canonical_id, normalized_url)
        - canonical_id: SHA256 hash prefixed with "cid:sha256:"
        - normalized_url: The cleaned, standardized URL
        
    Canonicalization Rules:
    1. Normalize scheme (default to http)
    2. Lowercase hostname
    3. Remove default ports (80 for http, 443 for https)
    4. Ensure trailing slash for directories
    5. Sort query parameters and remove tracking params
    6. Remove URL fragments (#section)
    
    Example:
        canonicalize("HTTPS://Example.COM:443/path?utm_source=google&id=123")
        Returns: ("cid:sha256:abc123...", "https://example.com/path?id=123")
    """
    # Parse the URL into components
    parsed = urlparse.urlsplit(raw_url)
    
    # Normalize scheme (protocol)
    scheme = parsed.scheme.lower() or "http"
    
    # Normalize hostname (convert to lowercase)
    hostname = parsed.hostname.lower() if parsed.hostname else ""
    
    # Handle port normalization (remove default ports)
    port_suffix = ""
    if parsed.port:
        # Only include port if it's not the default for the scheme
        is_default_port = (
            (scheme == "http" and parsed.port == 80) or
            (scheme == "https" and parsed.port == 443)
        )
        if not is_default_port:
            port_suffix = f":{parsed.port}"
    
    netloc = hostname + port_suffix
    
    # Normalize path
    path = parsed.path or "/"
    # Add trailing slash for directories (paths without file extensions)
    if not path.endswith("/") and "." not in path.split("/")[-1]:
        path += "/"
    
    # Clean and sort query parameters
    query_params = []
    if parsed.query:
        for param in parsed.query.split("&"):
            if param and not param.lower().startswith(("utm_", "gclid=")):
                # Remove tracking parameters (utm_*, gclid, etc.)
                query_params.append(param)
    
    # Sort query parameters for consistent ordering
    normalized_query = "&".join(sorted(query_params))
    
    # Reconstruct the normalized URL (without fragments)
    normalized_url = urlparse.urlunsplit((
        scheme, netloc, path, normalized_query, ""  # Empty fragment
    ))
    
    # Generate canonical ID using SHA256 hash
    url_hash = hashlib.sha256(normalized_url.encode('utf-8')).hexdigest()
    canonical_id = f"cid:sha256:{url_hash}"
    
    return canonical_id, normalized_url

# --- Rate Limiting and Politeness Controls ----------------------------------

class TokenBucket:
    """
    Token bucket algorithm for rate limiting requests per host.
    
    This implements respectful crawling by limiting the request rate to each host,
    preventing server overload and respecting robots.txt-style politeness.
    
    The token bucket allows burst requests up to the capacity, then refills
    tokens at a steady rate. This is more flexible than simple delays.
    
    Attributes:
        rate (float): Tokens added per second
        capacity (int): Maximum tokens that can be stored
        tokens (float): Current number of available tokens
        last_update (float): Timestamp of last token calculation
    """
    
    def __init__(self, rate_per_sec: float, burst_capacity: int = 1):
        """
        Initialize the token bucket.
        
        Args:
            rate_per_sec (float): How many tokens to add per second
            burst_capacity (int): Maximum tokens that can accumulate
            
        Example:
            TokenBucket(0.5, 2) allows 1 request every 2 seconds,
            with ability to burst up to 2 requests if tokens have accumulated
        """
        self.rate = rate_per_sec
        self.capacity = burst_capacity
        self.tokens = float(burst_capacity)  # Start with full bucket
        self.last_update = time.monotonic()
    
    def allow_request(self) -> bool:
        """
        Check if a request is allowed and consume a token if available.
        
        Returns:
            bool: True if request is allowed, False if rate limited
            
        Algorithm:
        1. Calculate time elapsed since last check
        2. Add tokens based on elapsed time and rate
        3. Cap tokens at maximum capacity
        4. If at least 1 token available, consume it and allow request
        5. Otherwise, deny request (rate limited)
        """
        current_time = time.monotonic()
        time_elapsed = current_time - self.last_update
        
        # Add tokens based on elapsed time
        tokens_to_add = time_elapsed * self.rate
        self.tokens = min(self.capacity, self.tokens + tokens_to_add)
        self.last_update = current_time
        
        # Check if we can consume a token
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True  # Request allowed
        
        return False  # Rate limited

# --- Priority Queue Item for Frontier ---------------------------------------

@dataclass(order=True)
class CrawlItem:
    """
    Represents a URL in the crawl frontier with priority scoring.
    
    The @dataclass(order=True) decorator allows these items to be compared
    and sorted in the priority queue based on the score field.
    
    Attributes:
        priority_score (float): Lower values = higher priority (min-heap)
        hostname (str): Host for rate limiting
        canonical_id (str): Unique identifier for deduplication
        url (str): The actual URL to crawl
        depth (int): How many links away from seed URLs
    """
    priority_score: float
    hostname: str
    canonical_id: str
    url: str
    depth: int

# --- Main Frontier Service --------------------------------------------------

class FrontierService:
    """
    The Frontier Service manages the crawl queue with intelligent prioritization.
    
    This is the core of the web crawler's microservice architecture, handling:
    - URL queue management with priority scoring
    - Deduplication using canonical IDs
    - Depth limiting to prevent infinite crawls
    - Rate limiting per host for politeness
    - Integration points for other microservices
    
    In production, this would integrate with:
    - Redis/MongoDB for persistent seen set
    - Kafka/RabbitMQ for inter-service messaging
    - Elasticsearch for content indexing
    - S3/HDFS for content archival
    """
    
    def __init__(self, max_depth: int = 4, default_rate_limit: float = 0.5):
        """
        Initialize the Frontier Service.
        
        Args:
            max_depth (int): Maximum crawl depth from seed URLs
            default_rate_limit (float): Default requests per second per host
        """
        # Priority queue for URLs to crawl (min-heap)
        self.crawl_queue: List[CrawlItem] = []
        
        # Deduplication: In production, use Redis with TTL
        self.seen_urls: Set[str] = set()
        
        # Content deduplication: In production, use bloom filters + LSH
        self.content_hashes: Set[str] = set()
        
        # Rate limiting per hostname
        self.rate_limiters: Dict[str, TokenBucket] = {}
        
        # Crawl configuration
        self.max_depth = max_depth
        self.default_rate_limit = default_rate_limit
        
        # Statistics for monitoring
        self.stats = {
            'urls_enqueued': 0,
            'urls_crawled': 0,
            'urls_deduplicated': 0,
            'rate_limited': 0
        }
    
    def calculate_priority_score(self, depth: int, freshness: float = 1.0, 
                                host_importance: float = 1.0) -> float:
        """
        Calculate priority score for URL scheduling.
        
        Lower scores = higher priority (min-heap behavior)
        
        Args:
            depth (int): Distance from seed URLs (0 = seed)
            freshness (float): How recently this URL was discovered (0-1)
            host_importance (float): Importance of the host domain (0-1)
            
        Returns:
            float: Priority score (lower = higher priority)
            
        Scoring Algorithm:
        - 60% weight on depth (prefer shallow pages)
        - 25% weight on freshness (prefer recently discovered)
        - 15% weight on host importance (prefer important domains)
        
        This can be extended with:
        - PageRank scores
        - Content type preferences
        - User engagement metrics
        - Business priority rules
        """
        # Depth component: prefer shallow pages (closer to seeds)
        depth_score = 1.0 / (1.0 + depth)
        
        # Combine weighted factors
        combined_score = (
            0.6 * depth_score +
            0.25 * freshness +
            0.15 * host_importance
        )
        
        # Return inverted score for min-heap (lower = higher priority)
        return 1.0 - combined_score
    
    async def enqueue_url(self, url: str, depth: int, 
                         freshness: float = 1.0) -> Optional[CrawlItem]:
        """
        Add a URL to the crawl frontier with deduplication and depth checking.
        
        Args:
            url (str): The URL to enqueue
            depth (int): Crawl depth of this URL
            freshness (float): Freshness score (0-1)
            
        Returns:
            Optional[CrawlItem]: The created crawl item, or None if rejected
            
        Rejection Reasons:
        1. Depth exceeds maximum limit
        2. URL already seen (duplicate)
        3. URL canonicalization fails
        """
        try:
            # Canonicalize URL for deduplication
            canonical_id, normalized_url = canonicalize(url)
            
            # Extract hostname for rate limiting
            parsed = urlparse.urlsplit(normalized_url)
            hostname = parsed.hostname or "unknown"
            
            # Check depth limit
            if depth > self.max_depth:
                print(f"  [DEPTH_LIMIT] Rejected {url} (depth {depth} > {self.max_depth})")
                return None
            
            # Check for duplicates
            if canonical_id in self.seen_urls:
                self.stats['urls_deduplicated'] += 1
                print(f"  [DUPLICATE] Already seen {canonical_id[:16]}...")
                return None
            
            # Mark as seen
            self.seen_urls.add(canonical_id)
            
            # Calculate priority score
            priority_score = self.calculate_priority_score(depth, freshness)
            
            # Create crawl item
            crawl_item = CrawlItem(
                priority_score=priority_score,
                hostname=hostname,
                canonical_id=canonical_id,
                url=normalized_url,
                depth=depth
            )
            
            # Add to priority queue
            heapq.heappush(self.crawl_queue, crawl_item)
            
            # Initialize rate limiter for new hosts
            if hostname not in self.rate_limiters:
                self.rate_limiters[hostname] = TokenBucket(
                    rate_per_sec=self.default_rate_limit,
                    burst_capacity=1
                )
                print(f"  [NEW_HOST] Added rate limiter for {hostname}")
            
            self.stats['urls_enqueued'] += 1
            print(f"  [ENQUEUED] {url} (depth={depth}, score={priority_score:.3f})")
            return crawl_item
            
        except Exception as e:
            print(f"  [ERROR] Failed to enqueue {url}: {e}")
            return None
    
    async def get_next_crawl_item(self) -> Optional[CrawlItem]:
        """
        Get the next URL to crawl, respecting rate limits.
        
        This method implements the core scheduling logic:
        1. Iterate through priority queue
        2. Find highest priority item whose host allows requests
        3. Remove and return that item
        4. If no items are available due to rate limiting, return None
        
        Returns:
            Optional[CrawlItem]: Next item to crawl, or None if rate limited
        """
        # Check each item in priority order
        for i, item in enumerate(self.crawl_queue):
            rate_limiter = self.rate_limiters.get(item.hostname)
            
            if rate_limiter and rate_limiter.allow_request():
                # Remove item from queue and re-heapify
                removed_item = self.crawl_queue.pop(i)
                heapq.heapify(self.crawl_queue)
                
                print(f"  [LEASED] {removed_item.url} (depth={removed_item.depth})")
                return removed_item
        
        # All items are rate limited
        if self.crawl_queue:
            self.stats['rate_limited'] += 1
            print(f"  [RATE_LIMITED] {len(self.crawl_queue)} items waiting")
        
        return None
    
    async def process_crawled_content(self, crawl_result: dict):
        """
        Process the results of a crawled page and extract new URLs.
        
        This method receives crawled content from the fetcher/parser microservices
        and schedules discovered links for future crawling.
        
        Args:
            crawl_result (dict): Results from crawling, including:
                - canonical_id: Unique identifier
                - url: The crawled URL
                - depth: Current depth
                - outlinks: List of discovered URLs
                - content_hash: Hash for content deduplication
                - metadata: Additional extracted data
        
        Integration Points:
        - Content Indexer: Send text content for search indexing
        - Archival Service: Store page snapshots
        - Analytics Service: Extract insights and metrics
        - Copyright Monitor: Check for protected content
        """
        current_depth = crawl_result.get("depth", 0)
        discovered_links = crawl_result.get("outlinks", [])
        content_hash = crawl_result.get("content_hash")
        
        # Content deduplication
        if content_hash and content_hash in self.content_hashes:
            print(f"  [CONTENT_DUP] Duplicate content detected")
            return
        
        if content_hash:
            self.content_hashes.add(content_hash)
        
        # Schedule discovered links for crawling
        print(f"  [PROCESSING] Found {len(discovered_links)} outlinks at depth {current_depth}")
        
        for link_url in discovered_links:
            await self.enqueue_url(link_url, depth=current_depth + 1)
        
        self.stats['urls_crawled'] += 1
        
        # In production, send to other microservices:
        # await self.send_to_indexer(crawl_result)
        # await self.send_to_archiver(crawl_result)
        # await self.send_to_analytics(crawl_result)
    
    def get_crawler_stats(self) -> dict:
        """
        Get current crawler statistics for monitoring and debugging.
        
        Returns:
            dict: Current statistics including queue size, seen URLs, etc.
        """
        return {
            **self.stats,
            'queue_size': len(self.crawl_queue),
            'seen_urls': len(self.seen_urls),
            'active_hosts': len(self.rate_limiters),
            'content_hashes': len(self.content_hashes)
        }

# --- Simulated Microservice Integration -------------------------------------

async def simulate_fetcher_service(crawl_item: CrawlItem) -> dict:
    """
    Simulate the fetcher microservice that downloads web pages.
    
    In production, this would be a separate service that:
    - Downloads HTML content
    - Handles redirects and errors
    - Respects robots.txt
    - Manages user agents and headers
    - Handles different content types
    
    Args:
        crawl_item (CrawlItem): The item to fetch
        
    Returns:
        dict: Simulated fetch results
    """
    # Simulate network delay
    await asyncio.sleep(0.1)
    
    # Simulate fetched content
    return {
        "canonical_id": crawl_item.canonical_id,
        "url": crawl_item.url,
        "depth": crawl_item.depth,
        "status_code": 200,
        "content_type": "text/html",
        "html_content": f"<html><body>Content from {crawl_item.url}</body></html>",
        "headers": {"Content-Type": "text/html", "Content-Length": "1024"}
    }

async def simulate_parser_service(fetch_result: dict) -> dict:
    """
    Simulate the parser microservice that extracts data from web pages.
    
    In production, this would:
    - Parse HTML/XML content
    - Extract text, links, images, metadata
    - Handle different document formats
    - Perform content analysis
    - Generate content hashes for deduplication
    
    Args:
        fetch_result (dict): Results from fetcher service
        
    Returns:
        dict: Parsed content with extracted links and metadata
    """
    # Simulate parsing delay
    await asyncio.sleep(0.05)
    
    # Extract hostname for generating realistic outlinks
    parsed_url = urlparse.urlsplit(fetch_result["url"])
    base_host = parsed_url.hostname or "example.com"
    
    # Generate simulated outlinks (in production, extract from HTML)
    outlinks = [
        f"https://{base_host}/page1",
        f"https://{base_host}/page2",
        f"https://{base_host}/section/article",
        f"https://external-site.com/related"
    ]
    
    # Simulate content hash for deduplication
    content_hash = hashlib.md5(fetch_result["html_content"].encode()).hexdigest()
    
    return {
        "canonical_id": fetch_result["canonical_id"],
        "url": fetch_result["url"],
        "depth": fetch_result["depth"],
        "outlinks": outlinks,
        "content_hash": content_hash,
        "title": f"Page Title for {fetch_result['url']}",
        "text_content": f"Extracted text content from {fetch_result['url']}",
        "metadata": {
            "word_count": 150,
            "language": "en",
            "last_modified": "2024-01-15T10:30:00Z"
        }
    }

# --- Main Crawler Orchestration ---------------------------------------------

async def run_web_crawler_demo(max_urls: int = 25, max_depth: int = 3):
    """
    Main orchestration function demonstrating the web crawler microservice architecture.
    
    This function simulates a complete crawling pipeline:
    1. Initialize frontier service
    2. Seed with initial URLs
    3. Process crawl queue with rate limiting
    4. Simulate fetching and parsing
    5. Extract and schedule new URLs
    6. Monitor progress and statistics
    
    Args:
        max_urls (int): Maximum URLs to crawl before stopping
        max_depth (int): Maximum crawl depth
    """
    print("=== Web Crawler Depth Microservice Architecture Demo ===\n")
    
    # Initialize the frontier service
    print("1. Initializing Frontier Service...")
    frontier = FrontierService(max_depth=max_depth, default_rate_limit=0.5)
    
    # Seed the crawler with initial URLs
    print("\n2. Seeding crawler with initial URLs...")
    seed_urls = [
        "https://example.com/",
        "https://news.example.com/",
        "https://blog.example.com/"
    ]
    
    for seed_url in seed_urls:
        await frontier.enqueue_url(seed_url, depth=0)
    
    print(f"   Seeded with {len(seed_urls)} URLs")
    
    # Main crawling loop
    print("\n3. Starting crawl processing loop...")
    crawled_count = 0
    
    while crawled_count < max_urls:
        # Get next item to crawl (respects rate limits)
        crawl_item = await frontier.get_next_crawl_item()
        
        if not crawl_item:
            # No items available due to rate limiting
            print("   [WAITING] All hosts rate limited, sleeping...")
            await asyncio.sleep(0.2)
            continue
        
        try:
            # Simulate fetcher microservice
            print(f"\n   [FETCHING] {crawl_item.url}")
            fetch_result = await simulate_fetcher_service(crawl_item)
            
            # Simulate parser microservice
            print(f"   [PARSING] Extracting content and links...")
            parse_result = await simulate_parser_service(fetch_result)
            
            # Process results and schedule new URLs
            print(f"   [SCHEDULING] Processing discovered links...")
            await frontier.process_crawled_content(parse_result)
            
            crawled_count += 1
            
            # Show progress
            if crawled_count % 5 == 0:
                stats = frontier.get_crawler_stats()
                print(f"\n   [PROGRESS] Crawled: {stats['urls_crawled']}, "
                      f"Queue: {stats['queue_size']}, "
                      f"Seen: {stats['seen_urls']}")
            
        except Exception as e:
            print(f"   [ERROR] Failed to process {crawl_item.url}: {e}")
        
        # Small delay to make output readable
        await asyncio.sleep(0.1)
    
    # Final statistics
    print("\n4. Crawl completed! Final statistics:")
    final_stats = frontier.get_crawler_stats()
    for key, value in final_stats.items():
        print(f"   {key}: {value}")
    
    print("\n=== Microservice Architecture Components Demonstrated ===")
    print("✓ Frontier Service: URL queue management with priority scoring")
    print("✓ Depth Service: Controlled depth-limited crawling")
    print("✓ Seen Service: URL deduplication using canonical IDs")
    print("✓ Rate Limiting: Respectful crawling with token buckets")
    print("✓ Content Processing: Simulated fetch, parse, and extraction")
    print("✓ Monitoring: Statistics and progress tracking")
    
    print("\n=== Production Integration Points ===")
    print("• Redis/MongoDB: Persistent seen set and queue storage")
    print("• Kafka/RabbitMQ: Inter-service messaging and event streaming")
    print("• Elasticsearch: Content indexing for search")
    print("• S3/HDFS: Content archival and preservation")
    print("• Prometheus/Grafana: Metrics and monitoring")
    print("• Docker/Kubernetes: Service orchestration and scaling")

# --- Entry Point ------------------------------------------------------------

if __name__ == "__main__":
    """
    Entry point for the web crawler demonstration.
    
    This demonstrates a production-ready web crawler architecture with:
    - Microservice separation of concerns
    - Asynchronous processing for performance
    - Rate limiting for respectful crawling
    - Deduplication for efficiency
    - Depth control for focused crawling
    - Monitoring and statistics
    
    Usage:
        python documented_web_crawler.py
    
    The demo will crawl up to 25 URLs with a maximum depth of 3,
    showing the interaction between all microservice components.
    """
    try:
        # Run the crawler demonstration
        asyncio.run(run_web_crawler_demo(max_urls=25, max_depth=3))
    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Crawler stopped by user")
    except Exception as e:
        print(f"\n[ERROR] Crawler failed: {e}")
        raise