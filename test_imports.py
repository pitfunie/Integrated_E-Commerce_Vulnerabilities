#!/usr/bin/env python3

print("Testing imports...")

# Test basic imports
try:
    import requests
    print("✓ requests imported successfully")
except ImportError as e:
    print(f"✗ requests import failed: {e}")

# Test OpenSearch imports
try:
    from opensearchpy import OpenSearch
    print("✓ opensearchpy.OpenSearch imported successfully")
except ImportError as e:
    print(f"✗ opensearchpy.OpenSearch import failed: {e}")

try:
    from opensearchpy.helpers import bulk
    print("✓ opensearchpy.helpers.bulk imported successfully")
except ImportError as e:
    print(f"✗ opensearchpy.helpers.bulk import failed: {e}")

# Test other common imports
try:
    import json
    print("✓ json imported successfully")
except ImportError as e:
    print(f"✗ json import failed: {e}")

try:
    from collections import defaultdict
    print("✓ collections.defaultdict imported successfully")
except ImportError as e:
    print(f"✗ collections.defaultdict import failed: {e}")

print("\nImport test complete!")