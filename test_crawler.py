#!/usr/bin/env python3
"""
Unit Tests for Enhanced Web Crawler
"""

import unittest
import asyncio
import tempfile
import os
from enhanced_web_crawler import (
    CrawlerConfig, CrawlerDB, canonicalize, TokenBucket,
    EnhancedFrontierService, ContentParser
)

class TestCrawlerComponents(unittest.TestCase):
    
    def test_canonicalize(self):
        """Test URL canonicalization"""
        cid1, norm1 = canonicalize("HTTPS://Example.COM:443/path?utm_source=test&id=123")
        cid2, norm2 = canonicalize("https://example.com/path?id=123")
        
        # Should produce same canonical ID after normalization
        self.assertEqual(norm1, "https://example.com/path?id=123")
        self.assertEqual(norm2, "https://example.com/path?id=123")
        self.assertEqual(cid1, cid2)
    
    def test_token_bucket(self):
        """Test rate limiting"""
        bucket = TokenBucket(rate_per_sec=2.0, burst=1)
        
        # First request should be allowed
        self.assertTrue(bucket.allow())
        
        # Second immediate request should be denied
        self.assertFalse(bucket.allow())
        
        # Check stats
        stats = bucket.get_stats()
        self.assertEqual(stats['requests_made'], 1)
        self.assertEqual(stats['requests_denied'], 1)
    
    def test_database(self):
        """Test database operations"""
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            db = CrawlerDB(tmp.name)
            
            # Test saving crawl result
            result = {
                'canonical_id': 'test_id',
                'url': 'https://example.com',
                'depth': 1,
                'status_code': 200,
                'content_type': 'text/html',
                'title': 'Test Page',
                'content_hash': 'abc123',
                'outlinks': ['https://example.com/page1']
            }
            
            db.save_crawl_result(result)
            
            # Check stats
            stats = db.get_stats()
            self.assertEqual(stats['total_urls'], 1)
            self.assertEqual(stats['total_links'], 1)
            
            # Cleanup
            os.unlink(tmp.name)
    
    def test_content_parser(self):
        """Test HTML parsing"""
        parser = ContentParser()
        
        fetch_result = {
            'url': 'https://example.com',
            'status_code': 200,
            'content': '<html><head><title>Test</title></head><body><a href="https://example.com/page1">Link</a></body></html>'
        }
        
        parsed = parser.parse(fetch_result)
        
        self.assertEqual(parsed['title'], 'Test')
        self.assertIn('https://example.com/page1', parsed['outlinks'])
        self.assertEqual(parsed['link_count'], 1)

class TestAsyncComponents(unittest.TestCase):
    
    def setUp(self):
        self.config = CrawlerConfig(max_depth=2, max_urls=5)
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            self.db_path = tmp.name
        self.db = CrawlerDB(self.db_path)
    
    def tearDown(self):
        os.unlink(self.db_path)
    
    def test_frontier_enqueue(self):
        """Test frontier URL enqueuing"""
        async def run_test():
            frontier = EnhancedFrontierService(self.config, self.db)
            
            # Enqueue URL
            item = await frontier.enqueue("https://example.com", depth=0)
            self.assertIsNotNone(item)
            self.assertEqual(item.depth, 0)
            
            # Duplicate should be rejected
            duplicate = await frontier.enqueue("https://example.com", depth=0)
            self.assertIsNone(duplicate)
            
            # Check stats
            stats = frontier.get_stats()
            self.assertEqual(stats['urls_enqueued'], 1)
            self.assertEqual(stats['duplicates_skipped'], 1)
        
        asyncio.run(run_test())

if __name__ == '__main__':
    unittest.main()