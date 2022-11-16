import heapq
import json
import math
import os
import pickle
import re
import sys
from collections import OrderedDict, defaultdict
from time import perf_counter
from typing import Dict, List

from bs4 import BeautifulSoup
from nltk.stem import PorterStemmer

from posting import Posting

DATA_URLS = "DEV"
STORAGE = "storage"
MAX_SIZE = 5000000 #5MB

disk_index = 0
doc_id = 0
doc_id_to_url = {}
stemmer = PorterStemmer()

index: Dict[str, List[Posting]] = defaultdict(list)
tempIndex: Dict[str, List[Posting]] = defaultdict(list)
outFile = open("out.txt", "w")



#also milestone #2
#PROXIMITY CHECKING
def checkQuery(searchList,count,i,checkLength, value,ranking,minranking):

    if i == len(searchList):
        if count == checkLength:
            return min(ranking,minranking)
        return minranking
    for val in searchList[i]:
        if val > value and val - value <= 5:
            minranking = checkQuery(searchList, count+1,i+1,checkLength, val, ranking + val - value, minranking)
      
    return minranking
        
    


#milestone #2
def answerQuery():
    query = input("Enter in a query: ")

    print("Processing...\n")
    queryTokenized = tokenize(query, True)
    
    queryList = set()

    for val in queryTokenized:
        for v in index[val]:
            queryList.add(v)

    retList = []
    for val in queryList:
        if(val.getQueryCount() == len(queryTokenized)):
           searchList = val.getCombo()

           for value in searchList[0]:
               ranking = checkQuery(searchList, 1, 1, len(queryTokenized),value,0,1e8)
               heapq.heappush(retList, (ranking, doc_id_to_url[val.getID()]))

    count = 0
    for val in retList:
        if count == 5: break
        print(val[1])
        count += 1
           

    
        
    
    


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

            # tokenize here
            tokens = tokenize(soup.get_text(), False)
            # add frequencies
            add_meta_data(doc_id, tokens)

            if(sys.getsizeof(index) > MAX_SIZE):
                offload_index()
            doc_id += 1

    offload_index()

    file_name = f"storage/url_map/urls.pickle"
    os.makedirs(os.path.dirname(file_name), exist_ok=True)

    with open(file_name, "wb") as f:
        pickle.dump(doc_id_to_url, f, protocol=pickle.HIGHEST_PROTOCOL)

def tokenize(text_content: str, askingQuery) -> Dict[str, int]:
    ret = dict()
    position = 1
    queryget = []
    for token in re.findall(r'[a-zA-Z0-9]+', text_content.lower()):
        token = stemmer.stem(token)

        if not askingQuery:
            if not token:
                print("empty")
            if token not in ret:
                ret[token] = [1, {position}]
            else:
                ret[token][0] += 1
                ret[token][1].add(position)
            position += 1
        else:
            queryget.append(token)
    return ret if not askingQuery else queryget

def add_meta_data(doc_id: int, tokens: Dict[str, int]) -> None:
    for token, data in tokens.items():
        heapq.heappush(index[token], Posting(doc_id, data[0],data[1]))

def offload_index() -> None:
    global index
    global disk_index

    file_name = f"storage/partial_index{disk_index}.pickle"
    os.makedirs(os.path.dirname(file_name), exist_ok=True)

    with open(file_name, "wb") as f:
        pickle.dump(OrderedDict(sorted(index.items())), f, protocol=pickle.HIGHEST_PROTOCOL)

    index.clear()
    disk_index += 1

# analysis question #1
def number_of_indexed() -> int:
    return doc_id

# analysis question #2
def unique_tokens() -> int:
    return len(index)

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

    #loading the first file
    with open('storage/partial_index0.pickle', 'rb') as f:
        index = pickle.load(f)  # Update contents of file0 to the dictionary

    #goint thru all the files
    for i in range(1, disk_index):
        with open(f'storage/partial_index{i}.pickle', 'rb') as f:
            tempIndex = pickle.load(f)  #getting the temp file

            for key, value in tempIndex.items():
                if(key in index):
                    for j in value:
                        heapq.heappush(index[key], j)  #Update contents of file1 to the dictionary
                else:
                    index[key] = value

    # file_name = f"storage/index.pickle"
    # os.makedirs(os.path.dirname(file_name), exist_ok=True)
    #
    # with open(file_name, "wb") as f:
    #     pickle.dump(index, f, protocol=pickle.HIGHEST_PROTOCOL)

    for k, v in index.items():
        outFile.write(k + " - " + str(len(v)) + '\n')

def main():
    

    build_index(DATA_URLS)
    merge_files()
    t_start = perf_counter()
    #print("Number of indexed: " + str(number_of_indexed()) + '\n')
    #print("Unique Tokens: " + str(unique_tokens()) + '\n')
    #print("Index size: " + str(get_index_size(STORAGE)) + '\n')
    
    # file_name = f"storage/partial_index{0}.pickle"
    # os.makedirs(os.path.dirname(file_name), exist_ok=True)

    # with open(file_name, "rb") as f:
    #     index: Dict[str, List[Posting]] = defaultdict(list, pickle.load(f))

    # file_name = f"storage/url_map/urls.pickle"
    # os.makedirs(os.path.dirname(file_name), exist_ok=True)

    # with open(file_name, "rb") as f:
    #     doc_id_to_url: Dict[int, str] = defaultdict(str, pickle.load(f))

    # print(len(index["uci"]))
    # print(doc_id_to_url[2])
    # for posting in index["uci"]:
    #     if posting.docid == 2:
    #         print(posting.tfidf)
    
    ''' example code for ordered dict serialization
    dict1 = {"z": 1, "b": 3, "a": 1, "as": 2, "bb": 1, "asdf": 1, "xcv": 1}
    with open("tmp.pickel", "wb") as f:
        pickle.dump(OrderedDict(sorted(dict1.items())), f, protocol=pickle.HIGHEST_PROTOCOL)
    
    with open("tmp.pickel", "rb") as f:
        tmp = dict(pickle.load(f)) 
    
    print(dict1)
    print(tmp)
    '''

    answerQuery()
    t_end = perf_counter()
    print(t_end - t_start)

if __name__ == "__main__":
    main()
