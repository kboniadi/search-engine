import json
import os
import pickle
import re
import sys
import heapq
from collections import defaultdict
from time import perf_counter
from typing import Dict, List

from bs4 import BeautifulSoup
from nltk.stem import PorterStemmer

from posting import Posting

DATA_URLS = "DEV"
MAX_SIZE = 5000000 #5MB

disk_index = 0
doc_id = 0
doc_id_to_url = {}
stemmer = PorterStemmer()

index: Dict[str, List[Posting]] = defaultdict(list)

def build_index(root_dir: str) -> None:
    global doc_id

    for (root, _, files) in os.walk(root_dir, topdown=True):
        for file in files:
            if not file.lower().endswith("json"):
                continue
            with open(os.path.join(root, file), "r") as f:
                data = json.load(f)
            data_url = data["url"]
            data_content = data["content"]

            doc_id_to_url[doc_id] = data_url
            
            soup = BeautifulSoup(data_content, "lxml")

            # tokenize here
            tokens = tokenize(soup.get_text())
            # add frequencies
            add_meta_data(doc_id, tokens)

            if(sys.getsizeof(index) > MAX_SIZE):
                offload_index()
            doc_id += 1

def tokenize(text_content: str) -> Dict[str, int]:
    ret = defaultdict(int)

    for token in re.findall(r'[a-zA-Z0-9]+', text_content):
        token = stemmer.stem(token)

        if not token:
            print("empty")
        ret[token] += 1
    return ret

def add_meta_data(doc_id: int, tokens: Dict[str, int]):
    for token, freq in tokens.items():
        heapq.heappush(index[token], Posting(doc_id, freq))

def offload_index():
    global index
    global disk_index

    file_name = f"storage/partial{disk_index}.pickle"
    os.makedirs(os.path.dirname(file_name), exist_ok=True)

    with open(file_name, "wb") as f:
        pickle.dump(index, f, protocol=pickle.HIGHEST_PROTOCOL)

    index.clear()
    disk_index += 1

#analysis question #1
def number_of_indexed():
    count = 0
    for postings in index.values():
        count += len(postings)
    return count

#analysis question #2
def unique_tokens():
    return len(index)

def main():
    t_start = perf_counter()
    build_index(DATA_URLS)
    t_end = perf_counter()
    print(t_end - t_start)

if __name__ == "__main__":
    main()