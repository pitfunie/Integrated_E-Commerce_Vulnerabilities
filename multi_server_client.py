import os
from opensearchpy import OpenSearch
try:
    from dotenv import load_dotenv
    load_dotenv('.env.multi')
except ImportError:
    pass  # dotenv not installed

def create_multi_server_client():
    """Create OpenSearch client with multiple servers and different credentials."""
    
    # Build server list from environment variables
    servers = []
    for i in range(1, 4):  # Servers 1, 2, 3
        host = os.getenv(f"OPENSEARCH_HOST_{i}")
        port = int(os.getenv(f"OPENSEARCH_PORT_{i}", 9200))
        user = os.getenv(f"OPENSEARCH_USER_{i}")
        password = os.getenv(f"OPENSEARCH_PASSWORD_{i}")
        
        if host and user and password:
            servers.append({
                "host": host,
                "port": port,
                "http_auth": (user, password),
                "use_ssl": True,
                "verify_certs": False
            })
    
    if not servers:
        raise ValueError("No OpenSearch servers configured")
    
    # OpenSearch client with multiple servers
    client = OpenSearch(servers)
    return client