from __future__ import annotations
from typing import List


class Posting:
    def __init__(self, docid: int, tfidf: int, positions,fields: List[str] = None):
        self.docid = docid
        self.tfidf = tfidf # use freq counts for now
        self.fields = fields
        self.positions = positions
        self.combo = [positions]
        self.queryCount = 1
    
    def __lt__(self, other: Posting):
        return self.docid < other.docid

    def __eq__(self, other):
        if self.docid == other.getID():
            self.setCombo(other.getPos())
            self.queryCount += 1
            return True
        return False
    
    def setCombo(self, value):
        self.combo.append(value)
    
    def getPos(self):
        return self.positions
        
    def getID(self):
        return self.docid
        
    def getCombo(self):
        x = list(self.combo)
        self.combo = [self.getPos()]
        return x
    
    def getQueryCount(self):
        x = self.queryCount
        self.queryCount = 1
        return x
        
    def __hash__(self):
         return hash(self.getID())
