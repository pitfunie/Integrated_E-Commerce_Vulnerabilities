from opensearchpy import OpenSearch
import json

# Connect to OpenSearch
client = OpenSearch([
    {'host': 'localhost', 'port': 9200}
])

# Index mapping for inverted index optimization
mapping = {
    "mappings": {
        "properties": {
            "content": {
                "type": "text",
                "analyzer": "standard"
            },
            "doc_id": {"type": "keyword"}
        }
    }
}

# Create index with mapping
client.indices.create(index="inverted_index", body=mapping)

# Bulk index 37 documents
documents = []
for doc_id in range(1, 38):  # 37 documents
    doc = {
        "_index": "inverted_index",
        "_id": doc_id,
        "_source": {
            "doc_id": f"doc_{doc_id}",
            "content": f"sample content for document {doc_id} with various terms"
        }
    }
    documents.append(doc)

# Bulk insert
from opensearchpy.helpers import bulk
bulk(client, documents)

# Search inverted index
def search_terms(terms):
    query = {
        "query": {
            "bool": {
                "should": [
                    {"match": {"content": term}} for term in terms
                ]
            }
        },
        "size": 37
    }
    
    response = client.search(index="inverted_index", body=query)
    
    # Return document IDs and scores
    results = []
    for hit in response['hits']['hits']:
        results.append({
            'doc_id': hit['_source']['doc_id'],
            'score': hit['_score']
        })
    
    return results

# Example search
search_results = search_terms(['sample', 'document'])
print(f"Found {len(search_results)} matching documents")

# Get index statistics (shows inverted index structure)
stats = client.indices.stats(index="inverted_index")
print(f"Total terms: {stats['indices']['inverted_index']['total']['store']['size_in_bytes']}")