from __future__ import annotations
from typing import List


class Posting:
    def __init__(self, docid: int, count: int, term):
        self.docid = docid
        self.count = count
        self.term = term
        self.allCount = [count]
        self.queryList = [term]
    
    def __lt__(self, other: Posting):
        return self.docid < other.docid

    def __eq__(self, other):
        if self.docid == other.getID():
            self.giveAllCount(other.getCount())
            self.giveQueryList(other.getTerm())
            return True
        return False

        
    def getID(self):
        return self.docid

    def getTerm(self):
        return self.term

    def getCount(self):
        return self.count

    def giveAllCount(self, cnt):
        self.allCount.append(cnt)

    def giveQueryList(self,trm):
        self.queryList.append(trm)

    def getQueryList(self):
        return self.queryList

    def getAllCount(self):
        return self.allCount
        
        
    def __hash__(self):
         return hash(self.getID())
