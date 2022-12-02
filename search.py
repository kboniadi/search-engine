
import math
import pickle
from collections import defaultdict
from time import perf_counter
from typing import List

from nltk.stem import PorterStemmer


class Search:
    BOOK_KEEPING_FILE = "storage/book-keeper/book-keeping.pickle"
    DOC_ID_TO_IRL_FILE = "storage/url_map/urls.pickle"

    def __init__(self) -> None:
        self.book_keeping = defaultdict(int)
        self.doc_id_to_url = {}
        self.doc_id = 0

        with open(Search.BOOK_KEEPING_FILE, 'rb') as f:
            self.book_keeping = pickle.load(f)
        
        with open(Search.DOC_ID_TO_IRL_FILE, 'rb') as f:
            self.doc_id_to_url = pickle.load(f)
        
        self.doc_id = len(self.doc_id_to_url)
    
    def transform_query(self, query):
        stemmer = PorterStemmer()
        return [stemmer.stem(word) for word in query]


    def query_index(self, query_list: List[int]):
        ranking_scores = defaultdict(float)
        with open("out1.txt", "r") as f:
            for val in query_list:
                if val in self.book_keeping:
                    try:
                        f.seek(self.book_keeping[val])
                        x = f.readline()
                        get_posting = x[1:-1].split(",")
                        get_posting = get_posting[1].split("|")

                        for i in range(1,len(get_posting)-1,3):
                            scale = 1

                            if int(get_posting[i+1]) == 1:
                                scale = 2

                            ranking_scores[int(get_posting[i-1])] += (1 + math.log10(int(get_posting[i])))*(math.log10(self.doc_id/(len(get_posting)//3))) * scale
                    except:
                        print("query failed")

        ranking_scores = sorted(ranking_scores, key = lambda x: -ranking_scores[x])

        count = 0
        ret = []
        for val in ranking_scores:
            if count == 5: break
            ret.append(self.doc_id_to_url[val])
            # print(ranking_scores[val])
            count += 1
        return ret


    def document_retrieval(self, query_list: List[str]):
        t_start = perf_counter()
        query_list_stemmed = self.transform_query(query_list)
        ret = self.query_index(query_list_stemmed)
        t_end = perf_counter()

        return (ret, round(t_end - t_start, 4))



def main():
    while True:
        search = Search()

        input1 = input("Get query? (Y/N): ")
        if input1 == "N": return
        query = input("Enter in a query: ")
        res, delay = search.document_retrieval(query.split())
        print(f"Delay: {delay} seconds")
        for doc in res:
            print(doc)
    
if __name__ == "__main__":
    main()
