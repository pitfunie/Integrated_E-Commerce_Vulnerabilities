try:
    from opensearchpy import OpenSearch
    from opensearchpy.helpers import bulk

    print("OpenSearch imports successful")
except ImportError as e:
    print(f"Import error: {e}")
