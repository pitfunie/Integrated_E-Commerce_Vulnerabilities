#!/usr/bin/env python3
"""
TTL-LRU Cache Implementation

This module implements a cache that combines two eviction strategies:
1. TTL (Time To Live) - removes items after they expire
2. LRU (Least Recently Used) - removes oldest items when capacity is full

Key Features:
- Automatic expiration of old entries based on TTL
- LRU eviction when cache reaches capacity
- O(1) get and set operations using OrderedDict
- Thread-safe for single-threaded applications

Author: Cache Implementation Demo
Date: 2024
"""

import time
from collections import OrderedDict


class TTL_LRU_Cache:
    """
    A cache that combines TTL (Time To Live) and LRU (Least Recently Used) eviction policies.
    
    This cache automatically removes items that have expired (TTL) and removes the least
    recently used items when the cache reaches its capacity limit (LRU).
    
    Attributes:
        capacity (int): Maximum number of items the cache can hold
        ttl (float): Time in seconds after which items expire
        cache (OrderedDict): Internal storage mapping keys to (value, timestamp) tuples
    """
    
    def __init__(self, capacity, ttl_seconds):
        """
        Initialize the TTL-LRU cache.
        
        Args:
            capacity (int): Maximum number of items to store in cache
            ttl_seconds (float): Time in seconds after which items expire
            
        Example:
            cache = TTL_LRU_Cache(capacity=3, ttl_seconds=5.0)
        """
        self.capacity = capacity
        self.ttl = ttl_seconds
        # OrderedDict maintains insertion order and allows O(1) move operations
        # Structure: key -> (value, timestamp)
        self.cache = OrderedDict()
    
    def _is_expired(self, timestamp):
        """
        Check if an item has expired based on its timestamp.
        
        Args:
            timestamp (float): Unix timestamp when item was stored
            
        Returns:
            bool: True if item has expired, False otherwise
            
        The expiration check compares current time with the stored timestamp
        plus the TTL duration.
        """
        current_time = time.time()
        age = current_time - timestamp
        return age > self.ttl
    
    def get(self, key):
        """
        Retrieve a value from the cache.
        
        Args:
            key: The key to look up
            
        Returns:
            The cached value if found and not expired, None otherwise
            
        Behavior:
        1. Return None if key doesn't exist
        2. Check if item has expired - if so, remove it and return None
        3. If valid, move item to end (mark as recently used) and return value
        
        Time Complexity: O(1)
        """
        # Check if key exists in cache
        if key not in self.cache:
            return None
        
        # Get stored value and timestamp
        value, timestamp = self.cache[key]
        
        # Check if item has expired
        if self._is_expired(timestamp):
            # Remove expired item from cache
            del self.cache[key]
            return None
        
        # Item is valid - move to end to mark as recently used
        # This is the "LRU" part - recently accessed items move to the end
        self.cache.move_to_end(key)
        return value
    
    def set(self, key, value):
        """
        Store a key-value pair in the cache.
        
        Args:
            key: The key to store
            value: The value to associate with the key
            
        Behavior:
        1. If key exists and is expired, remove the old entry
        2. If cache is at capacity and key is new, remove LRU item
        3. Store new item with current timestamp
        4. Move item to end (mark as most recently used)
        
        Time Complexity: O(1)
        """
        current_time = time.time()
        
        # Handle existing key
        if key in self.cache:
            _, timestamp = self.cache[key]
            # Remove if expired (will be re-added below)
            if self._is_expired(timestamp):
                del self.cache[key]
        
        # Check capacity - evict LRU item if needed
        # Only evict if this is a new key and we're at capacity
        if key not in self.cache and len(self.cache) >= self.capacity:
            # Remove least recently used item (first item in OrderedDict)
            # last=False means remove from beginning (oldest/LRU item)
            lru_key, lru_data = self.cache.popitem(last=False)
            print(f"  [EVICTED LRU] Removed '{lru_key}' to make space")
        
        # Store the new item with current timestamp
        self.cache[key] = (value, current_time)
        
        # Move to end to mark as most recently used
        self.cache.move_to_end(key)
        print(f"  [STORED] '{key}' = '{value}' (expires in {self.ttl}s)")
    
    def cleanup_expired(self):
        """
        Manually remove all expired items from the cache.
        
        This method is useful for proactive cleanup, though expired items
        are also removed automatically during get() operations.
        
        Returns:
            int: Number of expired items removed
        """
        expired_keys = []
        current_time = time.time()
        
        # Find all expired keys
        for key, (value, timestamp) in self.cache.items():
            if self._is_expired(timestamp):
                expired_keys.append(key)
        
        # Remove expired keys
        for key in expired_keys:
            del self.cache[key]
            print(f"  [EXPIRED] Removed '{key}'")
        
        return len(expired_keys)
    
    def get_stats(self):
        """
        Get cache statistics.
        
        Returns:
            dict: Statistics including size, capacity, and expired items
        """
        current_time = time.time()
        expired_count = 0
        
        for key, (value, timestamp) in self.cache.items():
            if self._is_expired(timestamp):
                expired_count += 1
        
        return {
            'size': len(self.cache),
            'capacity': self.capacity,
            'expired_items': expired_count,
            'utilization': f"{len(self.cache)}/{self.capacity} ({len(self.cache)/self.capacity*100:.1f}%)"
        }
    
    def __repr__(self):
        """
        String representation of the cache showing current key-value pairs.
        
        Returns:
            str: Dictionary-like representation of cache contents
            
        Note: Only shows values, not timestamps, for cleaner output
        """
        return str({k: v[0] for k, v in self.cache.items()})


def demonstrate_ttl_lru_cache():
    """
    Comprehensive demonstration of TTL-LRU cache functionality.
    
    This function shows:
    1. Basic get/set operations
    2. LRU eviction when capacity is exceeded
    3. TTL expiration after time passes
    4. Interaction between TTL and LRU policies
    """
    print("=== TTL-LRU Cache Demonstration ===\n")
    
    # Create cache with capacity=3 and TTL=3 seconds
    print("1. Creating cache (capacity=3, TTL=3 seconds)")
    cache = TTL_LRU_Cache(capacity=3, ttl_seconds=3.0)
    print(f"   Initial cache: {cache}")
    print(f"   Stats: {cache.get_stats()}")
    
    print("\n2. Adding items to cache...")
    # Add items - should fit within capacity
    cache.set("user:1", "Alice")
    cache.set("user:2", "Bob") 
    cache.set("user:3", "Charlie")
    print(f"   Cache after adding 3 items: {cache}")
    
    print("\n3. Testing LRU eviction...")
    # This should evict "user:1" (least recently used)
    cache.set("user:4", "Diana")
    print(f"   Cache after adding 4th item: {cache}")
    
    print("\n4. Testing cache hits...")
    # Access items to change LRU order
    result = cache.get("user:2")
    print(f"   get('user:2') = {result}")
    result = cache.get("user:1")  # Should be None (evicted)
    print(f"   get('user:1') = {result} (was evicted)")
    
    print(f"   Cache after access: {cache}")
    
    print("\n5. Waiting for TTL expiration...")
    print("   Sleeping for 4 seconds to let items expire...")
    time.sleep(4)  # Wait longer than TTL
    
    print("\n6. Testing TTL expiration...")
    # These should return None due to expiration
    result = cache.get("user:2")
    print(f"   get('user:2') after TTL = {result} (expired)")
    result = cache.get("user:3")
    print(f"   get('user:3') after TTL = {result} (expired)")
    
    print(f"   Cache after TTL cleanup: {cache}")
    
    print("\n7. Adding new items after expiration...")
    cache.set("user:5", "Eve")
    cache.set("user:6", "Frank")
    print(f"   Cache with new items: {cache}")
    
    print("\n8. Final statistics...")
    stats = cache.get_stats()
    print(f"   Final stats: {stats}")
    
    print("\n=== Demonstration Complete ===")


def performance_test():
    """
    Simple performance test showing O(1) operations.
    """
    print("\n=== Performance Test ===")
    
    cache = TTL_LRU_Cache(capacity=1000, ttl_seconds=60)
    
    # Time set operations
    start_time = time.time()
    for i in range(1000):
        cache.set(f"key_{i}", f"value_{i}")
    set_time = time.time() - start_time
    
    # Time get operations
    start_time = time.time()
    for i in range(1000):
        cache.get(f"key_{i}")
    get_time = time.time() - start_time
    
    print(f"1000 SET operations: {set_time:.4f} seconds ({set_time/1000*1000:.2f} μs per op)")
    print(f"1000 GET operations: {get_time:.4f} seconds ({get_time/1000*1000:.2f} μs per op)")
    print("Both operations are O(1) - constant time complexity")


if __name__ == "__main__":
    """
    Main execution block.
    
    Runs the comprehensive demonstration and performance test.
    This shows real-world usage patterns and validates the cache behavior.
    """
    try:
        demonstrate_ttl_lru_cache()
        performance_test()
    except KeyboardInterrupt:
        print("\nDemo interrupted by user")
    except Exception as e:
        print(f"Error during demonstration: {e}")
        raise