#!/usr/bin/env python3
import sys
import re

WORD_RE = re.compile(r"\w+")


def mapper():
    for line in sys.stdin:
        docid, text = line.strip().split("\t", 1)
        for pos, word in enumerate(WORD_RE.findall(text)):
            term = word.lower()
            print(f"{term}\t{docid}:{pos}")


if __name__ == "__main__":
    mapper()
