import csv
import json
import math
import os
import pickle
import re
import sys
from collections import defaultdict
from time import perf_counter

import mrjob
from bs4 import BeautifulSoup
from mrjob.job import MRJob
from mrjob.step import MRStep
from nltk.stem import PorterStemmer

DATA_URLS = "DEV"
STORAGE = "storage"
MAX_SIZE = 5000000 #5MB 

disk_index = 0
doc_id = 0
doc_id_to_url = {}
stemmer = PorterStemmer()

index = defaultdict(str)
tempIndex = defaultdict(str)
bookKeeping = defaultdict(int)

class MergeIndex(MRJob):
   OUTPUT_PROTOCOL = mrjob.protocol.JSONValueProtocol
   def mapper(self, _, line):
       key, value = line.split(",")
       yield key, value

   def reducer(self, key, values):
      values = "".join(str(v) for v in values)
      ret = key + "," + values
      yield None,ret

def build_index(root_dir: str) -> None:
    global doc_id
    global doc_id_to_url

    for (root, _, files) in os.walk(root_dir):
        for file in files:
            if not file.lower().endswith("json"):
                continue
            with open(os.path.join(root, file), "r") as f:
                data = json.load(f)
            data_url = data["url"]
            data_content = data["content"]

            doc_id_to_url[doc_id] = data_url

            soup = BeautifulSoup(data_content, "lxml")

            #important words
            impF = open("imp.txt", "a")

            impStr = ""

            for a in ["h1", "h2", "h3", "title", "strong", "b"]:
                for words in soup.find_all(a):
                    impStr = impStr + words.text + " "


            impTokens = []

            if impStr != "":
                impTokens = tokenize(impStr, False)

            # impF.write(str(doc_id) + '\n\n')
            # for imp in impTokens:
            #     impF.write(imp)
            #     impF.write('\n')
            #
            # impF.write('\n')
            # impF.write('\n')
            #
            # impF.close()


            # tokenize here
            tokens = tokenize(soup.get_text(), False)
            # add frequencies
            add_meta_data(doc_id, tokens, impTokens)

            if(sys.getsizeof(index) > MAX_SIZE):
                offload_index()
            doc_id += 1

    #offloading the remaining index
    offload_index()

    file_name = f"storage/url_map/urls.pickle"
    os.makedirs(os.path.dirname(file_name), exist_ok=True)

    with open(file_name, "wb") as f:
        pickle.dump(doc_id_to_url, f, protocol=pickle.HIGHEST_PROTOCOL)


def tokenize(text_content, askingQuery):
    ret = defaultdict(int)
    queryget = []
    for token in re.findall(r'[a-zA-Z0-9]+', text_content.lower()):
        token = stemmer.stem(token)

        if not askingQuery:
            if not token:
                print("empty")
            # add book keeper secondary index
            ret[token] += 1
        else:
            queryget.append(token)
    return ret if not askingQuery else queryget


def add_meta_data(doc_id, tokens, impTokens):
    for token, data in tokens.items():
        isImp = 0
        if token in impTokens:
            isImp = 1
        index[token] += str(doc_id) +"|" +str(data)+"|" + str(isImp) + "|"


def offload_index() -> None:
    global index
    global disk_index

    file_name = f"storage/partial_index{disk_index}.pickle"
    os.makedirs(os.path.dirname(file_name), exist_ok=True)

    with open(file_name, "wb") as f:
        pickle.dump(index, f, protocol=pickle.HIGHEST_PROTOCOL)

    index.clear()
    disk_index += 1

# analysis question #1
def number_of_indexed() -> int:
    global doc_id
    return doc_id

# analysis question #2
def unique_tokens() -> int:
    return len(bookKeeping)

# analysis question #3: The total size (in KB) of your index on disk
def get_index_size(root_dir: str) -> str:
    count = 0

    for (root, _, files) in os.walk(root_dir):
        for file in files:
            count += os.stat(os.path.join(root, file)).st_size
    return convert_size(count, "KB")

def convert_size(size_bytes, unit="B"):
    if size_bytes == 0:
        return "0B"

    size_name = {"B": 0, "KB": 1, "MB": 2, "GB": 3, "TB": 4, "PB": 5, "EB": 6, "ZB": 7, "YB": 8}

    if unit not in size_name:
        raise ValueError("Must select from \
        ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB]")
        
    i = size_name[unit]
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return "%s %s" % (s, unit)


def merge_files():
    global index
    global tempIndex  
    
    with open("out.txt", "w") as f:
        for i in range(disk_index):
            with open(f'storage/partial_index{i}.pickle', 'rb') as f1:
                tempIndex = pickle.load(f1)  #getting the temp file
                for key, value in tempIndex.items():
                   f.write(key + "," + value + "\n")
                   

    MergeIndex.run()

    offset = 0
    with open("out1.txt", "r") as f:
       while True:
          x = f.readline()
          if not x: break
          term = x[1:-1].split(",")
          bookKeeping[term[0]] = offset
          offset += len(x)


def main():
    
    t_start = perf_counter()
    build_index(DATA_URLS)
    merge_files()

    file_name = f"storage/book-keeper/book-keeping.pickle"
    os.makedirs(os.path.dirname(file_name), exist_ok=True)

    with open(file_name, "wb") as f:
        pickle.dump(bookKeeping, f, protocol=pickle.HIGHEST_PROTOCOL)

    t_end = perf_counter()
    print("Build took:", t_end-t_start)
    
    print("Number of indexed: " + str(number_of_indexed()) + '\n')
    print("Unique Tokens: " + str(unique_tokens()) + '\n')
    print("Index size: " + str(get_index_size(STORAGE)) + '\n')

if __name__ == "__main__":
    main()
