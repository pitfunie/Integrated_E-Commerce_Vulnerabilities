from opensearchpy import OpenSearch
import os

def show_actual_inverted_index(client, index_name):
    """
    Show the actual inverted index structure that OpenSearch creates.
    This reveals what 'type: text' actually produces internally.
    """
    
    # Get term vectors (shows actual inverted index for a document)
    response = client.termvectors(
        index=index_name,
        id=1,  # Document ID
        fields=['content'],
        term_statistics=True,
        field_statistics=True
    )
    
    print("=== ACTUAL INVERTED INDEX STRUCTURE ===")
    print("What OpenSearch created from 'type: text':\n")
    
    if 'term_vectors' in response and 'content' in response['term_vectors']:
        terms = response['term_vectors']['content']['terms']
        
        for term, stats in terms.items():
            doc_freq = stats.get('doc_freq', 0)  # How many docs contain this term
            term_freq = stats.get('term_freq', 0)  # How often in this doc
            
            print(f"Term: '{term}'")
            print(f"  → Appears in {doc_freq} documents")
            print(f"  → Frequency in this doc: {term_freq}")
            print()
    
    # Show what the 'standard' analyzer actually does
    analyze_response = client.indices.analyze(
        index=index_name,
        body={
            "analyzer": "standard",
            "text": "Python Programming is Fun!"
        }
    )
    
    print("=== WHAT 'STANDARD' ANALYZER DOES ===")
    print("Input: 'Python Programming is Fun!'")
    print("Tokenized output:")
    
    for token in analyze_response['tokens']:
        print(f"  '{token['token']}' (position: {token['position']})")

def demonstrate_real_inverted_index():
    """Show the actual inverted index that gets created."""
    
    # Connect to OpenSearch
    client = OpenSearch(
        [{"host": "localhost", "port": 9200}],
        http_auth=(os.getenv("OPENSEARCH_USER", "admin"), os.getenv("OPENSEARCH_PASSWORD", "MyStrongPassword123!")),
        use_ssl=True,
        verify_certs=False
    )
    
    index_name = "inverted_index_demo"
    
    # Show what the mapping actually creates
    show_actual_inverted_index(client, index_name)

if __name__ == "__main__":
    demonstrate_real_inverted_index()