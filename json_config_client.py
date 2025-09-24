import os
import json
from opensearchpy import OpenSearch

def create_json_config_client():
    """Create OpenSearch client from JSON configuration."""
    
    servers_json = os.getenv("OPENSEARCH_SERVERS")
    if not servers_json:
        raise ValueError("OPENSEARCH_SERVERS environment variable not set")
    
    server_configs = json.loads(servers_json)
    
    # Build OpenSearch server list
    servers = []
    for config in server_configs:
        servers.append({
            "host": config["host"],
            "port": config["port"],
            "http_auth": (config["user"], config["password"]),
            "use_ssl": True,
            "verify_certs": False
        })
    
    client = OpenSearch(servers)
    return client