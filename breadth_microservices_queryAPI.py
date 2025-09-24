# query_api.py
from fastapi import FastAPI, Query
from redis import Redis
from search_engine import search_shards, rank_results
from metadata import get_metadata

app = FastAPI()
cache = Redis(host="localhost", port=6379)


@app.get("/search")
def search(term: str, compound: bool = False):
    if cached := cache.get(term):
        return {"source": "cache", "results": cached}

    results = search_shards(term, compound)
    ranked = rank_results(results)
    cache.set(term, ranked)
    return {"source": "live", "results": ranked}


@app.get("/metadata")
def metadata(term: str):
    return get_metadata(term)
