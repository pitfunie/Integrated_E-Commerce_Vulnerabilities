"""
OpenSearch Inverted Index Implementation

This module demonstrates how to create and use an inverted index with OpenSearch
for efficient document searching. An inverted index maps each unique term to
the list of documents containing that term.

Example inverted index structure:
Term "python" -> [doc_1, doc_5, doc_12]
Term "machine" -> [doc_2, doc_5, doc_8]
Term "learning" -> [doc_2, doc_5, doc_15]

Dependencies: pip install opensearch-py
"""

from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk
import json
import os


def create_opensearch_client():
    """
    Create connection to OpenSearch cluster.

    Returns:
        OpenSearch: Connected client instance

    Note: This connects to localhost. For AWS OpenSearch, use:
    client = OpenSearch([
        {'host': 'your-domain.us-east-1.es.amazonaws.com', 'port': 443, 'use_ssl': True}
    ])
    """
    client = OpenSearch(
        [{"host": "localhost", "port": 9200}],
        http_auth=(
            os.getenv("OPENSEARCH_USER", "admin"),
            os.getenv("OPENSEARCH_PASSWORD", "MyStrongPassword123!"),
        ),
        use_ssl=True,
        verify_certs=False,
    )
    return client


def create_index_with_mapping(client, index_name):
    """
    Create an OpenSearch index with optimized mapping for inverted index.

    Args:
        client (OpenSearch): OpenSearch client
        index_name (str): Name of the index to create

    The mapping defines how documents are analyzed and stored:
    - "text" type creates inverted index automatically
    - "standard" analyzer tokenizes text into terms
    - "keyword" type stores exact values (not analyzed)
    """
    mapping = {
        "mappings": {  # This is the value for "mappings" in the index definition
            "properties": {  # This is the value for "properties" in the mapping definition
                "content": {  # Field to be indexed
                    "type": "text",  # Creates inverted index
                    "analyzer": "standard",  # Tokenizes: "Hello World" -> ["hello", "world"]
                },
                "doc_id": {"type": "keyword"},  # Exact match only, not analyzed
                "title": {"type": "text", "analyzer": "standard"},
            }
        }
    }

    # Delete index if it exists (for clean start) taking care of deduplication
    if client.indices.exists(index=index_name):
        client.indices.delete(index=index_name)
        print(f"Deleted existing index: {index_name}")

    # Create new index with mapping
    client.indices.create(index=index_name, body=mapping)
    print(f"Created index: {index_name}")


def bulk_index_documents(client, index_name, num_docs=37):
    """
    Index multiple documents at once using bulk operation.

    Args:
        client (OpenSearch): OpenSearch client
        index_name (str): Target index name
        num_docs (int): Number of documents to create (default: 37)

    Bulk indexing is much faster than indexing documents one by one.
    Each document gets automatically processed to build the inverted index.
    """
    documents = []

    # Sample content to simulate real documents
    sample_contents = [
        "machine learning algorithms and data science techniques",
        "python programming for web development and automation",
        "database management systems with SQL and NoSQL",
        "artificial intelligence and neural network architectures",
        "cloud computing services on AWS and Azure platforms",
        "software engineering best practices and design patterns",
        "data structures and algorithms for efficient programming",
        "cybersecurity threats and protection mechanisms",
        "mobile app development for iOS and Android platforms",
        "blockchain technology and cryptocurrency applications",
    ]

    # Create documents for bulk indexing
    for doc_id in range(1, num_docs + 1):
        # Cycle through sample content to create variety
        content_index = (doc_id - 1) % len(sample_contents)
        content = sample_contents[content_index]

        # Each document needs _index, _id, and _source
        doc = {
            "_index": index_name,  # Which index to store in
            "_id": doc_id,  # Unique document ID
            "_source": {  # Actual document content
                "doc_id": f"doc_{doc_id}",
                "title": f"Document {doc_id}",
                "content": f"{content} document number {doc_id}",
            },
        }
        documents.append(doc)

    # Perform bulk indexing
    print(f"Indexing {len(documents)} documents...")
    success_count, failed_items = bulk(client, documents)
    print(f"Successfully indexed: {success_count} documents")

    # Wait for indexing to complete
    client.indices.refresh(index=index_name)
    print("Index refresh completed")


def search_inverted_index(client, index_name, query_terms):
    """
    Search the inverted index for documents containing specified terms.

    Args:
        client (OpenSearch): OpenSearch client
        index_name (str): Index to search
        query_terms (list): List of terms to search for

    Returns:
        list: List of matching documents with scores

    How inverted index search works:
    1. OpenSearch looks up each term in the inverted index
    2. Finds all documents containing those terms
    3. Calculates relevance scores based on term frequency
    4. Returns ranked results
    """
    # Build search query using boolean logic
    query = {
        "query": {
            "bool": {
                "should": [  # OR logic: match any term
                    {"match": {"content": term}} for term in query_terms
                ],
                "minimum_should_match": 1,  # At least one term must match
            }
        },
        "size": 50,  # Maximum results to return
        "_source": ["doc_id", "title", "content"],  # Fields to return
        "highlight": {"fields": {"content": {}}},  # Highlight matching terms
    }

    print(f"Searching for terms: {query_terms}")
    response = client.search(index=index_name, body=query)

    # Process and return results
    results = []
    total_hits = response["hits"]["total"]["value"]
    print(f"Found {total_hits} matching documents")

    for hit in response["hits"]["hits"]:
        result = {
            "doc_id": hit["_source"]["doc_id"],
            "title": hit["_source"]["title"],
            "score": hit["_score"],  # Relevance score
            "content": hit["_source"]["content"][:100] + "...",  # Truncated content
        }

        # Add highlighted text if available
        if "highlight" in hit:
            result["highlighted"] = hit["highlight"]["content"][0]

        results.append(result)

    return results


def get_index_statistics(client, index_name):
    """
    Get statistics about the inverted index structure.

    Args:
        client (OpenSearch): OpenSearch client
        index_name (str): Index name

    Returns:
        dict: Index statistics including term count and storage info
    """
    # Get index stats
    stats = client.indices.stats(index=index_name)
    index_stats = stats["indices"][index_name]["total"]

    # Get index settings and mappings
    settings = client.indices.get(index=index_name)

    return {
        "document_count": index_stats["docs"]["count"],
        "storage_size_bytes": index_stats["store"]["size_in_bytes"],
        "storage_size_mb": round(
            index_stats["store"]["size_in_bytes"] / (1024 * 1024), 2
        ),
        "indexing_operations": index_stats["indexing"]["index_total"],
        "search_operations": index_stats["search"]["query_total"],
    }


def demonstrate_inverted_index():
    """
    Main demonstration function showing inverted index operations.
    """
    print("=== OpenSearch Inverted Index Demo ===\n")

    # Step 1: Connect to OpenSearch
    print("1. Connecting to OpenSearch...")
    client = create_opensearch_client()

    # Step 2: Create index with proper mapping
    print("\n2. Creating index with inverted index mapping...")
    index_name = "inverted_index_demo"
    create_index_with_mapping(client, index_name)

    # Step 3: Index documents (builds inverted index automatically)
    print("\n3. Indexing documents...")
    bulk_index_documents(client, index_name, num_docs=37)

    # Step 4: Perform searches using inverted index
    print("\n4. Searching inverted index...")

    # Search example 1: Single term
    results = search_inverted_index(client, index_name, ["machine"])
    print(f"\nSearch results for 'machine':")
    for result in results[:3]:  # Show top 3 results
        print(
            f"  {result['doc_id']}: {result['content']} (score: {result['score']:.2f})"
        )

    # Search example 2: Multiple terms
    results = search_inverted_index(client, index_name, ["python", "programming"])
    print(f"\nSearch results for 'python' OR 'programming':")
    for result in results[:3]:
        print(
            f"  {result['doc_id']}: {result['content']} (score: {result['score']:.2f})"
        )

    # Step 5: Show index statistics
    print("\n5. Index statistics...")
    stats = get_index_statistics(client, index_name)
    print(f"Documents indexed: {stats['document_count']}")
    print(f"Storage size: {stats['storage_size_mb']} MB")
    print(f"Total indexing operations: {stats['indexing_operations']}")

    print("\n=== Demo Complete ===")


if __name__ == "__main__":
    """
    Run the inverted index demonstration.

    This script shows:
    1. How to create an OpenSearch index optimized for inverted indexing
    2. How to bulk index documents (37 docs with ~3900 unique terms)
    3. How to search using the inverted index
    4. How to get statistics about the index structure

    The inverted index allows fast term-to-document lookups:
    - Term lookup: O(1)
    - Document retrieval: O(k) where k = number of matching docs
    - Much faster than scanning all documents linearly
    """
    try:
        demonstrate_inverted_index()
    except Exception as e:
        print(f"Error: {e}")
        print("Make sure OpenSearch is running on localhost:9200")
        print(
            "Or install with: docker run -p 9200:9200 opensearchproject/opensearch:latest"
        )
