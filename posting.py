from __future__ import annotations
from typing import List


class Posting:
    def __init__(self, docid: int, count: int):
        self.docid = docid
        self.count = count
    
    def __lt__(self, other: Posting):
        return self.docid < other.docid

    def __eq__(self, other):
        return self.docid == other.getID()
  
    def getID(self):
        return self.docid

    def getCount(self):
        return self.count
        
    def __hash__(self):
         return hash(self.getID())
