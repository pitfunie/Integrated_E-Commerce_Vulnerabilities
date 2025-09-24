from collections import defaultdict


def reducer():
    current_term = None
    postings = []

    for line in sys.stdin:
        term, doc_pos = line.strip().split("\t")
        if term != current_term:
            if current_term:
                print(f"{current_term}\t{','.join(postings)}")
            current_term = term
            postings = []
        postings.append(doc_pos)

    if current_term:
        print(f"{current_term}\t{','.join(postings)}")
