import json
import os
import re
from collections import defaultdict
from time import perf_counter
from typing import Dict, List

from bs4 import BeautifulSoup
from nltk.stem import PorterStemmer

DATA_URLS = "DEV"

doc_id = 0
index_indexer = {}
stemmer = PorterStemmer()

def crawl():
    pass

def generate_posting(root_dir: str) -> None:
    global doc_id

    for (root, dirs, files) in os.walk(root_dir, topdown=True):
        for file in files:
            if not file.lower().endswith("json"):
                continue
            with open(os.path.join(root, file), "r") as f:
                data = json.load(f)
            data_url = data["url"]
            data_content = data["content"]

            index_indexer[doc_id] = data_url
            soup = BeautifulSoup(data_content, "lxml")

            # tokenize here
            tokens = tokenize(soup.get_text())
            # add frequencies
            doc_id += 1

def tokenize(text_content: str) -> Dict[str, int]:
    ret = defaultdict(int)

    for token in re.findall(r'[a-zA-Z0-9]+', text_content):
        token = stemmer.stem(token)

        if not token:
            print("empty")
        ret[token] += 1
    return ret

def main():
    t_start = perf_counter()
    generate_posting(DATA_URLS)
    t_end = perf_counter()
    print(t_start - t_end)

if __name__ == "__main__":
    main()