for line in sys.stdin:
    meta, text = line.strip().split("\t", 1)
    trace_id, user_id, timestamp = meta.split(",")
    docid = f"{trace_id}:{user_id}:{timestamp}"
    for pos, word in enumerate(WORD_RE.findall(text)):
        term = word.lower()
        print(f"{term}\t{docid}:{pos}")
"""
shard_id = hash(docid) % num_shards

"""
