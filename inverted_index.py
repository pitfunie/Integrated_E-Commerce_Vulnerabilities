from collections import defaultdict
import re

class InvertedIndex:
    def __init__(self):
        self.index = defaultdict(set)  # term -> set of doc_ids
        self.documents = {}  # doc_id -> content
    
    def add_document(self, doc_id, content):
        self.documents[doc_id] = content
        terms = self._tokenize(content)
        
        for term in terms:
            self.index[term].add(doc_id)
    
    def _tokenize(self, text):
        return set(re.findall(r'\b\w+\b', text.lower()))
    
    def search(self, query_terms):
        if not query_terms:
            return set()
        
        # Get documents containing first term
        result = self.index[query_terms[0]].copy()
        
        # Intersect with documents containing other terms
        for term in query_terms[1:]:
            result &= self.index[term]
        
        return result
    
    def get_stats(self):
        return {
            'total_terms': len(self.index),
            'total_documents': len(self.documents),
            'avg_terms_per_doc': sum(len(self._tokenize(content)) 
                                   for content in self.documents.values()) / len(self.documents)
        }

# Create index with 37 documents and ~3900 terms
index = InvertedIndex()

# Sample documents (simulate 37 docs)
sample_docs = [
    "machine learning algorithms data science",
    "python programming web development",
    "database management systems sql",
    "artificial intelligence neural networks",
    "cloud computing aws services"
] * 8  # Repeat to get ~37 docs

for i, content in enumerate(sample_docs[:37]):
    index.add_document(f"doc_{i+1}", f"{content} document {i+1}")

# Search examples
results = index.search(['machine', 'learning'])
print(f"Documents with 'machine' AND 'learning': {results}")

results = index.search(['python'])
print(f"Documents with 'python': {results}")

# Statistics
stats = index.get_stats()
print(f"Index stats: {stats}")

# Show inverted index structure (sample)
print("\nSample inverted index entries:")
for term in list(index.index.keys())[:5]:
    print(f"'{term}' -> {index.index[term]}")