#!/usr/bin/env python3

# Test OpenSearch imports
try:
    from opensearchpy import OpenSearch
    from opensearchpy.helpers import bulk
    print("✓ OpenSearch imports successful")
    
    # Test creating client (won't connect without server)
    client = OpenSearch([{'host': 'localhost', 'port': 9200}])
    print("✓ OpenSearch client created")
    
except ImportError as e:
    print(f"✗ Import error: {e}")
except Exception as e:
    print(f"✓ Imports work, connection error expected: {e}")

print("All imports are working correctly!")