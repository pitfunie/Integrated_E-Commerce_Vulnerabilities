#!/usr/bin/env python3
import sys

"""
How this works:

Outputs each posting as soon as it is read, separated by commas, without storing all postings in a list.
Each term’s postings are printed on a single line, as soon as the term changes or input ends.
This approach is memory-efficient and suitable for very large posting lists.
"""


def reducer_streaming():
    """
    Reads term-posting pairs from standard input (one per line, tab-separated),
    and streams output for each term as postings are read, avoiding large in-memory lists.

    Input format (from stdin):
        term<TAB>docID:pos

    The input is assumed to be sorted by term (e.g., from a k-way merge in MapReduce).

    For each group of postings with the same term:
        - Output the term and its postings as a comma-separated list, streaming as you go.
        - Does not accumulate all postings in memory for a term.
    """
    current_term = None
    first_posting = True  # Tracks if we're at the first posting for a term

    for line in sys.stdin:
        term, posting = line.strip().split("\t", 1)
        if term != current_term:
            # If not the first term, close the previous line
            if current_term is not None:
                print()  # End the previous term's line
            # Start a new term's output
            print(f"{term}\t{posting}", end="")
            current_term = term
            first_posting = False
        else:
            # Continue the current term's postings, prepend comma
            print(f",{posting}", end="")
    # After all input, print a newline if any term was processed
    if current_term is not None:
        print()


if __name__ == "__main__":
    reducer_streaming()
