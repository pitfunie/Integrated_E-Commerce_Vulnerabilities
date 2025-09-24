#!/usr/bin/env python3
"""
Simple runner script for the enhanced web crawler
"""

import asyncio
import sys
from enhanced_web_crawler import EnhancedWebCrawler, CrawlerConfig

async def main():
    # Configuration
    config = CrawlerConfig(
        max_depth=2,
        max_urls=20,
        rate_limit=0.5,  # 1 request per 2 seconds per host
        timeout=10
    )
    
    # Create crawler
    crawler = EnhancedWebCrawler(config)
    
    # Seed URLs - using httpbin.org for testing (it's designed for HTTP testing)
    seed_urls = [
        "https://httpbin.org/",
        "https://httpbin.org/html",
        "https://example.com/"
    ]
    
    print("Starting Enhanced Web Crawler...")
    print(f"Max depth: {config.max_depth}")
    print(f"Max URLs: {config.max_urls}")
    print(f"Rate limit: {config.rate_limit} req/sec/host")
    print(f"Seed URLs: {len(seed_urls)}")
    print("-" * 50)
    
    try:
        await crawler.crawl(seed_urls)
        print("\nCrawling completed successfully!")
        
    except KeyboardInterrupt:
        print("\nCrawling interrupted by user")
        crawler.stop()
        
    except Exception as e:
        print(f"\nCrawling failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())