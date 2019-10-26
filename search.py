import sys
import re
import os
from nltk.corpus import stopwords
from Stemmer import Stemmer
stemmer =Stemmer("porter")
stopWords = set(stopwords.words('english'))
from datetime import datetime
from nltk import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
read_data={}
count = 0 
import random 

def split_string(str):
        t = str.find('t')
        i = str.find('i')
        e = str.find('e')
        c = str.find('c') 
        arr = []
        arr.append(str[t+2: i-1].split())
        arr.append(str[i+2: e-1].split())
        arr.append(str[e+2: c-1].split())
        arr.append(str[c+2:].split())
        return arr

def search_file_simple(query_tokens, index_path, query_files): 
    docs = []
    for i in range(len(query_tokens)):
        query_word = query_tokens[i]
        file = [open(i,'r') for i in query_files[i]]
        temp_docs=[]
        for i in file:
            line = i.readlines()         
            for j in range(len(line)):
                key, value = line[j].split('#')
                key_val, key_freq = key.split()
                
                if key_val ==  query_word:
                    read_data[key_val] = {'t':[], 'i':[], 'e':[], 'c':[]}
                    arr = split_string(value)
                    global count
                    count+=1
                    read_data[key_val]['t'] = list(zip(arr[0][0::2], arr[0][1::2]))
                    read_data[key_val]['i'] = list(zip(arr[1][0::2], arr[1][1::2]))
                    read_data[key_val]['e'] = list(zip(arr[2][0::2], arr[2][1::2]))
                    read_data[key_val]['c'] = list(zip(arr[3][0::2], arr[3][1::2])) 
                    # print(read_data[key_val])
                    for key in read_data[key_val]:
                        entry = read_data[key_val][key]
                        for sub_field in entry:
                            temp_docs.append(sub_field)
                            # print(sub_field)
        docs.append(temp_docs)
    if len(docs) >1:
        return list(set.intersection(*map(set,docs)))
    else:
        return docs

# def field_query(tokens, fields 


def print_pages(final_result, docs, file):
    # sorted_docs = sorted(docs,key=lambda x: x[1], reverse=True)
    for i in final_result:
        print(i)
    for i in docs[-(10-len(final_result)):]:
        print(file[len(docs) - int(i[0])].strip('\n'))

def search_in_title(word, file):
    results = []
    for i in file:
        if word in i.split():
            results.append(i)
    return results



def main():
    path_to_index = sys.argv[1]
    index_path = os.path.abspath(path_to_index)
    title_pat = os.path.join(index_path,'title.txt')
    print("Initialising Search Engine .....") 
    sec_file_path = os.path.join(index_path, 'secondary_index.txt')
    title_file = open(title_pat, 'r')
    title_file_lines = title_file.readlines()
    sec_file = open(sec_file_path, 'r') 
    start = sec_file.tell()
    text = sec_file.readlines()
    print("====")
    print("Done")
    while(1):
        results = []
        query = input('\nType in your query:\n')
        query = query.lower()
        title_results = []
        priority_pages = []

        if re.search(r'[t|b|c|e|i]:',query[:2]):
            print("Working with field query...\n")
            group_field = {'t':[], 'b':[],'c':[],'e':[],'i':[]}
            global_words = re.findall(r'[t|i|c|l|b]:([^:]*)(?!\S)', query)
            global_fields = re.findall(r'([t|i|c|l|b]):', query)
            c=0
            for i in global_fields:
                group_field[i] = (global_words[c].split())
                c=+1
            del c
            #search by title 
            if len(group_field['t'])>0:
                for i in group_field['t']:
                    title_results.append(search_in_title(i, title_file_lines))
                priority_pages = list(set.intersection(*map(set,title_results)))

                for i in title_results:
                    if(len(i) > 0):
                        if(len(priority_pages)<10):
                            priority_pages.append(i[0])
            if len(priority_pages)<6:
                query = re.sub(r'\s*\w+:(\w+)\s*', '', query, flags= re.MULTILINE)
                query_tokens = query.split()
                query_t = [stemmer.stemWord(w) for w in query_tokens if w not in stopWords]
                query_tokens = query_t            
                query_files = []
                for i in query_tokens:
                    file = []
                    for file_location in text:
                        # file = []
                        file_location = file_location.strip()
                        offsets, file_name = file_location.split(":")
                        start, end = offsets.split('-')
                        if start.strip() <= i[0] and i[0] <= end.strip():
                            file.append(file_name)   
                        del file_name
                    query_files.append(file)   
                    del file
                # print(query_tokens)
                doc= search_file_simple(query_tokens, index_path, query_files)
                # print(doc)
                print_pages(priority_pages, doc, title_file_lines)
            else:
                for i in priority_pages[:10]:
                    print(i)
            # for i in 
            # results = rank(results, docFreq, nfiles, 'f')
        else:
            query_tokens = query.split()
            query_t = [stemmer.stemWord(w) for w in query_tokens if w not in stopWords]
            query_tokens = query_t   
            query_files = []
            for i in query_tokens:
                title_results.append(search_in_title(i, title_file_lines))
            priority_pages = list(set.intersection(*map(set,title_results)))
            # print(len(priority_pages))
            for i in title_results:
                if(len(i)>0):
                    if(len(priority_pages)<10):
                        priority_pages.append(i[0])
            if len(priority_pages)<6: 
                query = re.sub(r'\s*\w+:(\w+)\s*', '', query, flags= re.MULTILINE) 
                query_tokens = query.split()
                query_t = [stemmer.stemWord(w) for w in query_tokens if w not in stopWords]
                query_tokens = query_t            
                query_files = []
                for i in query_tokens:
                    file = []
                    for file_location in text:
                        # file = []
                        file_location = file_location.strip()
                        offsets, file_name = file_location.split(":")
                        start, end = offsets.split('-')
                        if start.strip() <= i[0] and i[0] <= end.strip():
                            file.append(file_name)   
                        del file_name
                    query_files.append(file)   
                    del file
                doc= search_file_simple(query_tokens, index_path, query_files)
                print_pages(priority_pages, doc, title_file_lines)
            else:
                for i in priority_pages[:10]:
                    print(i)


if __name__ == '__main__':
    main()



