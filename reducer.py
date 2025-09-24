#!/usr/bin/env python3
import sys


def reducer():
    """
    Reads term-posting pairs from standard input (one per line, tab-separated),
    groups postings by term, and outputs the merged posting list for each term.

    Input format (from stdin):
        term<TAB>docID:pos

    The input is assumed to be sorted by term (e.g., from a k-way merge in MapReduce).

    For each group of postings with the same term:
        - Collect all postings for the term.
        - When the term changes, output the previous term and its postings.
        - At the end, output the last term and its postings.
    """
    current_term = None
    postings = []  # Collects docID:pos strings for the current term

    for line in sys.stdin:
        term, posting = line.strip().split("\t", 1)
        if term != current_term:
            # If we've moved to a new term, output the previous term's postings
            if current_term is not None:
                output_postings(current_term, postings)
            current_term = term
            postings = []
        postings.append(posting)
    # Output the last term's postings
    if current_term:
        output_postings(current_term, postings)


def output_postings(term, postings):
    """
    Outputs the term and its merged posting list as a single line.

    Args:
        term (str): The term (key).
        postings (List[str]): List of posting strings (e.g., "docID:pos").

    The postings are joined by commas and printed as:
        term<TAB>posting1,posting2,...
    """
    # Postings are already sorted due to k-way merge in the previous step
    posting_list = ",".join(postings)
    print(f"{term}\t{posting_list}")


if __name__ == "__main__":
    reducer()
