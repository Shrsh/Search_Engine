#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 28 19:08:30 2019

@author: harsh
"""

import xml.sax
import re
import os
from nltk.tokenize import  word_tokenize 
from nltk.corpus import stopwords
from Stemmer import Stemmer
from collections import Counter
import time
from threading import Thread
import queue
import sys
import math
import psutil
import heapq
import contextlib

output = {}
title = []
c=0

#reg expression 
info_box = "\{\{Infobox (.*?\n)*? *?\}\}"
category = "\[ *\[ *[cC]ategory *: *(.*?) *\] *\]"
references = "== *[Rr]eferences *==(.*?\n)+?\n"
ext_links = "== *[eE]xternal [lL]inks *== *(.*?\n)+?\n"

index_path = ""

count = 0 

symbols = ["<",">","\"","\n","\'","|","[","]","{","}",",","=","(",")","*",":","%","'",".","\"","#","$","+","-"]

stopWords = set(stopwords.words('english'))
stopWords.update('infobox', 'redirect','category')

stemmer =Stemmer("porter")
output_index = ""
title_path = ""

class data_Handler( xml.sax.ContentHandler ):
	
	def __init__(self):
		self.title = ""
		self.raw_text = ""
		self.cat_gory =""
		self.infobox = ""
		self.ref =""
		self.external_links =""
		self.text = ""
		self.CurrentData = ""
		self.url=""
	
	# Call when an element starts
	def startElement(self, tag, attributes):
		self.CurrentData = tag
			
	# Call when an elements ends
	def endElement(self, tag):
		global count 
		if self.CurrentData == "title":
			self.title =  re.sub(r'\—|\%|\$|\'|\||\.|\*|\[|\]|\:|\;|\,|\{|\}|\(|\)|\=|\+|\-|\_|\#|\!|\`|\"|\?|\/|\>|\<|\&|\\|\u2013|\n', r' ',self.title)
			title.append(self.title)
			count+=1
		elif self.CurrentData == "text":
			text = self.raw_text
			data_Handler.pre_process(self, text)
		self.CurrentData = ""
		self.raw_text = "" 
		self.title = ""
		self.cat_gory =""
		self.infobox = ""
		self.ref =""
		self.external_links =""
		
	# Call when a character is read
	def characters(self, content):
		if self.CurrentData == "title": 
			self.title += content.replace('\n', ' ').strip()
		elif self.CurrentData == "text":
			 self.raw_text += content
			 
	
	def cleanup(self,text):
		#tokenization
		if len(text)==0:
			return []
		else:
			text = re.sub(r'\—|\%|\$|\'|\||\.|\*|\[|\]|\:|\;|\,|\{|\}|\(|\)|\=|\+|\-|\_|\#|\!|\`|\"|\?|\/|\>|\<|\&|\\|\u2013|\n', r' ', text)
			#for i in symbols: 
			#	text = text.replace(i,"")
			text = text.split()
			#text = [re.sub('[^A-Za-z0-9]+', '',item) for item in text]
			text = [stemmer.stemWord(w) for w in text if w not in stopWords]
			return Counter(text)
	
	def search_infobox(self,text,info_box):
		try :    
			self.infobox = re.search(info_box,text).group(0)
		except AttributeError:
			self.infobox = ""  
	
	def search_category(self,text,category):
		try:
			cat_gory = re.findall(category,text)
			for i in cat_gory:
				self.cat_gory = self.cat_gory + " " + i
				text = text.replace(i,"")
			#print(self.cat_gory)
		except AttributeError:
			self.cat_gory = ""
	
	def search_external_links(self, text, ext_links):
		try:
			self.external_links = re.search(ext_links,text).group(0)
			#print(self.external_links)
		except AttributeError:
			self.external_links = ""
	
	def search_references(self, text, references):
		try:
			self.ref = re.search(references,text).group(0)
			#print(self.ref)
		except AttributeError:
			self.ref = ""
		
	def pre_process(self,raw_text):        
		raw_text = re.sub(r'http[^\ ]*\ ', r' ', raw_text)
		#raw_text = re.sub(r'^https?:\/\/.*[\r\n]*', '', raw_text, flags= re.MULTILINE)
		raw_text = re.sub(r'&nbsp;|&lt;|&gt;|&amp;|&quot;|&apos;', r' ', raw_text)
		raw_text = raw_text.lower()

		#extracting different fields
		thr1 = Thread(target=self.search_infobox, args=(raw_text,info_box)) 
		thr2 = Thread(target=self.search_category, args=(raw_text,category))
		thr3 = Thread(target=self.search_external_links, args=(raw_text,ext_links))
		thr4 = Thread(target=self.search_references, args=(raw_text,references))
		
		thr1.start()
		thr2.start()
		thr3.start()
		thr4.start()
		
		thr1.join()
		thr2.join()
		thr3.join()
		thr4.join()

	#simple text 
		if re.match(r"\W*(redirect)\W*", raw_text):
			self.text  = ""
		else:
			self.text = raw_text.replace(self.infobox,"").replace(self.ref,"").replace(self.external_links,"").replace(self.cat_gory,"")

	#Tokenisation Process
		data_dict ={}
		
		que = queue.Queue()
		threads_list = list()
		
		t1 = Thread(target=lambda q, arg1: q.put(self.cleanup(arg1)), args=(que,self.text))
		t2 = Thread(target=lambda q, arg1: q.put(self.cleanup(arg1)), args=(que,self.infobox))
		t4 = Thread(target=lambda q, arg1: q.put(self.cleanup(arg1)), args=(que,self.external_links))
		t5 = Thread(target=lambda q, arg1: q.put(self.cleanup(arg1)), args=(que,self.cat_gory))
		t1.start()
		t2.start()
		t4.start()
		t5.start()
		threads_list.append(t1)        
		threads_list.append(t2)
		threads_list.append(t4) 
		threads_list.append(t5)
		
		for t in threads_list:
			t.join()
		data_dict['text'] = que.get()
		data_dict['infobox'] = que.get()
		data_dict['external_links'] = que.get()
		data_dict['category'] = que.get()
		#print(data_dict['text'])

	#inverted index creation
		for x in data_dict:
			gen = (k for k in data_dict[x] if (len(k) <20 and len(k)>1))
			for k in gen:
				if k in output:
					if x == 'text':
						output[k]['t'].append([count,data_dict[x][k]])
					elif x == 'infobox':
						output[k]['i'].append([count,data_dict[x][k]])
					elif x == 'external_links':
						output[k]['e'].append([count,data_dict[x][k]])
					elif x == 'category':
						output[k]['c'].append([count,data_dict[x][k]])
				else:
					output[k] = {'t':[],'i':[],'e':[],'c':[]}
					if x == 'text':
						output[k]['t'].append([count,data_dict[x][k]])
					elif x == 'infobox':
						output[k]['i'].append([count,data_dict[x][k]])
					elif x == 'external_links':
						output[k]['e'].append([count,data_dict[x][k]])
					elif x == 'category':
						output[k]['c'].append([count,data_dict[x][k]])
		
		if count%5000 == 0:
			print(count)
			self.write_file_index()
		del(data_dict)

	def  write_file_index(self):
		for key in output:
			for key_sub in output[key]:
					output[key][key_sub].sort(key = lambda x: x[1])
		index = str(math.ceil(count/5000))+'index.txt' 
		#title_str = str(math.ceil(count/5000))+'title.txt'
		title_str = 'title.txt'
		output_pat = os.path.join(index_path, index)
		title_path = os.path.join(index_path, title_str)
		
		with open(output_pat, 'w+') as file:
			for key in sorted(output):
				file.write("{0}#".format(key)+"")
				for i in output[key]:
					file.write("{0}:".format(i)+" ")
					for i in output[key][i]:
						file.write( " ".join(map(str,i)) + " ")
				file.write("\n")
		file.close()
		output.clear()

	def merge_files(self,f_num):
		#number of files = f_num
		title_files = []
		index_files = []
		for i in range(f_num):
			n = str(i+1) + 'index.txt'
			n2  = str(i+1)  + 'title.txt'
			name_index = os.path.join(index_path, n)
			index_files.append(name_index)
			name_title = os.path.join(index_path, n2)
			title_files.append(name_title)
		n = self.merge_divide_files(index_files)
		for i in index_files:
			if os.path.isfile(i):
				os.remove(i)
			else:    ## Show an error ##
				print("Error: %s file not found" % i)
		return n 

	def merge_divide_files(self,files_arr):
		heap =[]
		prev_text = 0
		prev_pointer =0
		opened_files = [0]*len(files_arr) 
		file_content = [""]*len(files_arr)
		present_word = [""]*len(files_arr)
		files = [open(fn) for fn in files_arr]   
		for i in range(len(files)):
			opened_files[i] = 1
			temp_line = files[i].readline()
			file_content[i] = temp_line.split('#')[1]
			present_word[i] = temp_line.split('#')[0]
			if present_word[i] not in heap:
				heapq.heappush(heap, present_word[i]) 

		merged_index = os.path.join(index_path,'index0.txt')
		f = open(merged_index, 'w')
		thresh = 0 
		while(any(opened_files)):
			val = heapq.heappop(heap)
			
			thresh+=1
			if thresh % 20000 == 0:
				f.close()
				l = 'index' + str(math.ceil(thresh/20000)) + '.txt'
				merged_index = os.path.join(index_path,l)
				f = open(merged_index, 'w')

			temp_match_content = []
			for i in range(len(files)):
				if val == present_word[i]:
					temp_match_content.append(file_content[i])
					next_line = files[i].readline()
					if next_line:
						file_content[i] = next_line.split('#')[1]
						present_word[i] = next_line.split('#')[0]
						if present_word[i] not in heap:
							heapq.heappush(heap, present_word[i])
					else:
						#close the file
						opened_files[i] = 0
						files[i].close()
			merge_content = temp_match_content
			#merge the heap content
			if len(merge_content) > 1:
				merge_content = [self.split_string(i) for i in merge_content]
				#print(merge_content)
				data_dict = {'t':[],'i':[],'e':[],'c':[]}
				for i in merge_content: 
					data_dict['t'].append(list(zip(i[0][0::2], i[0][1::2])))
					data_dict['i'].append(list(zip(i[1][0::2], i[1][1::2])))
					data_dict['e'].append(list(zip(i[2][0::2], i[2][1::2])))
					data_dict['c'].append(list(zip(i[3][0::2], i[3][1::2])))
					
				#sort 
				for i in data_dict:
					data_dict[i] = [item for sublist in data_dict[i] for item in sublist]
					data_dict[i].sort(key = lambda x: x[1])
				s=0
				for i in data_dict.keys():
					s += sum(int(x[1]) for x in data_dict[i])
				f.write("{0}".format(val) + " " + str(s) + "#")  
				for i in data_dict:
					f.write("{0}:".format(i)+" ")
					for j in data_dict[i]:
						f.write( " ".join(map(str,j)) + " ")
				f.write("\n")
			else:
				merge_content = [self.split_string(i) for i in merge_content]
				#print(merge_content)
				data_dict = {'t':[],'i':[],'e':[],'c':[]}
				for i in merge_content: 
					data_dict['t'].append(list(zip(i[0][0::2], i[0][1::2])))
					data_dict['i'].append(list(zip(i[1][0::2], i[1][1::2])))
					data_dict['e'].append(list(zip(i[2][0::2], i[2][1::2])))
					data_dict['c'].append(list(zip(i[3][0::2], i[3][1::2])))

				for i in data_dict:
					data_dict[i] = [item for sublist in data_dict[i] for item in sublist]
				s=0
				for i in data_dict.keys():
					s += sum(int(x[1]) for x in data_dict[i])
				f.write("{0}".format(val) + " " + str(s) + "#")  
				for i in data_dict:
					f.write("{0}:".format(i)+" ")
					for j in data_dict[i]:
						f.write( " ".join(map(str,j)) + " ")
				f.write("\n")

				#f.write("{0}#".format(val)+"")
				#f.write(*merge_content)

		return int(math.ceil(thresh/20000))

	def split_string(self,str):
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

	def make_secondary_index(self, n):
		index_files = []
		for i in range(n):
			n_ = 'index' +str(i) + '.txt'
			name_index = os.path.join(index_path, n_)
			index_files.append(name_index)
		sec_index = os.path.join(os.path.abspath(index_path), "secondary_index.txt")
		f = open(sec_index, 'w')
		for file in index_files:
			with open(file, 'r') as fi:
				lineList = fi.readlines()
				f.write("{0}-{1}:".format(lineList[0][0], lineList[-1][0]))
				f.write("{0}".format(file))
			f.write("\n")
	
def build_index(path, output_path):
	start = time.process_time()   
	ch = data_Handler() 
	global index_path

	index_path = os.path.abspath(output_path)
	saxparser = xml.sax.make_parser()
	saxparser.setContentHandler(ch)
	saxparser.parse(os.path.abspath(path),)    
	ch.write_file_index()
	title_path = os.path.join(os.path.abspath(output_path),"title.txt")
	global title
	with open(title_path, 'w+') as f:
		for t in title:
			f.write("{0}".format(t)) 
			f.write("\n")
	f.close()
	title.clear()
	# Counting the number of files 
	f_n = math.ceil(count/5000)
	number_index = ch.merge_files(int(f_n))
	ch.make_secondary_index(number_index)
	print(time.process_time() - start)

def main():
	path_to_wikidump = sys.argv[1]
	output_path = sys.argv[2]
	build_index(path_to_wikidump,output_path)
		
if __name__ == '__main__':
	main()



		
		
		
	
		 
	
		
		
		
		
	
			 
			
   
  

   
		 
			