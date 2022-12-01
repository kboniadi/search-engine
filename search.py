
from collections import defaultdict
import math
import pickle
from time import perf_counter
from typing import List
from nltk.stem import PorterStemmer
import re

book_keeping = defaultdict(int)
doc_id_to_url = {}

def transform_query(query):
    stemmer = PorterStemmer()
    return [stemmer.stem(word) for word in query]


def query_index(query_list: List[int]):
    ranking_scores = defaultdict(float)
    with open("out1.txt", "r") as f:
        for val in query_list:
            if val in book_keeping:
                try:
                   f.seek(book_keeping[val])
                   x = f.readline()
                   get_posting = x[1:-1].split(",")
                   get_posting = get_posting[1].split("|")
                   for i in range(1,len(get_posting),2):
                       ranking_scores[int(get_posting[i-1])] += (1 + math.log10(int(get_posting[i])))*(math.log10(doc_id/(len(get_posting)//2)))
                except:
                   print("failed bruh")
    ranking_scores = sorted(ranking_scores, key = lambda x: -ranking_scores[x])

    count = 0
    ret = []
    for val in ranking_scores:
       if count == 5: break
       ret.append(doc_id_to_url[val])
       count += 1
    return ret                                                                         


def document_retrieval(query_list: List[str]):
    t_start = perf_counter()
    query_list_stemmed = transform_query(query_list)
    ret = query_index(query_list_stemmed)
    t_end = perf_counter()

    return (ret, round(t_end - t_start, 3))


def main():
    global book_keeping
    global doc_id_to_url
    global doc_id

    while True:
        book_keeking_file = "storage/book-keeper/book-keeping.pickle"
        doc_id_to_url_file = "storage/url_map/urls.pickle"

        with open(book_keeking_file, 'rb') as f:
            book_keeping = pickle.load(f)
        
        with open(doc_id_to_url_file, 'rb') as f:
            doc_id_to_url = pickle.load(f)
        
        doc_id = len(doc_id_to_url)

        input1 = input("Get query? (Y/N): ")
        if input1 == "N": return
        query = input("Enter in a query: ")
        res, delay = document_retrieval(query.split())
        print(f"Delay: {delay} seconds")
        for doc in res:
            print(doc)
    
if __name__ == "__main__":
    main()
