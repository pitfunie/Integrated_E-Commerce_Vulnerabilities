#!/usr/bin/env python3
import sys

"""
Batch Processing Reducer

This script reads term-posting pairs from standard input (one per line, tab-separated),
groups postings by term, and outputs the merged posting list for each term in batches.
Instead of holding all postings for a term in memory, it writes them out in fixed-size chunks (batches).

This approach is useful when a single term may have too many postings to fit in memory.
You can also adapt this pattern to write batches to a database or key-value store.

Example Usage:
    python3 reducer_batch.py < input.txt
"""

BATCH_SIZE = 100000  # Number of postings per batch (adjust as needed)


def reducer_batch():
    """
    Reads sorted term-posting pairs from stdin, groups by term, and outputs postings in batches.
    """
    current_term = None
    postings = []

    for line in sys.stdin:
        term, posting = line.strip().split("\t", 1)
        if term != current_term:
            # Output any remaining postings for the previous term
            if current_term is not None and postings:
                output_postings(current_term, postings)
            current_term = term
            postings = []
        postings.append(posting)
        # If batch size reached, output and clear batch
        if len(postings) >= BATCH_SIZE:
            output_postings(current_term, postings)
            postings = []
    # Output any remaining postings for the last term
    if current_term and postings:
        output_postings(current_term, postings)


def output_postings(term, postings):
    """
    Outputs a batch of postings for a term as a single line.

    Args:
        term (str): The term (key).
        postings (List[str]): List of posting strings (e.g., "docID:pos").
    """
    posting_list = ",".join(postings)
    print(f"{term}\t{posting_list}")


if __name__ == "__main__":
    reducer_batch()
