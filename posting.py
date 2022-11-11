from typing import List
from __future__ import annotations


class Posting:
    def __init__(self, docid: int, tfidf: int, fields: List[str], positions: List[str]):
        self.docid = docid
        self.tfidf = tfidf # use freq counts for now
        self.fields = fields
        self.positions = positions
    
    def __lt__(self, other: Posting):
        return self.docid < other.docid