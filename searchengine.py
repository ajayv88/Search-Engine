import urllib2
from bs4 import BeautifulSoup
from urlparse import urljoin
import pymongo
from pymongo import MongoClient
import time
import re
from nltk import PorterStemmer
from nltk.corpus import stopwords
import math
import multiprocessing as mp 
import datetime


client = MongoClient('mongodb://localhost:27017')

db = client['final_search_engine_cs2']

posts = db.posts
links = db.links
list_of_words = db.words

# def build_graph():


stopwords = list(set(stopwords.words('english')))

dic = {}
pages_indexed = {}
unwanted = ['/', '\\', '@', '+', ':','#']

class searchEngine:

	def __init__(self):
		print "Setting up a search engine ..."

	def clean_html(self, element):
		# print "yes"
		if element.parent.name in ['style', 'script', '[document]', 'head', 'title']:
			return False
		elif re.match('<!--.*-->', str(element.encode('utf-8'))):
			return False
		else:
			return True

	def remove_nonalphanum(self, result):
		for w in range(0,len(result)):
			for i in unwanted:
				if i in result[w]:
					# print "yes", w
					result[w] = " "

		clean_text = ' '.join(result).replace("\n", " ")
		return clean_text

	def social_link(self, link):
		global social
		for i in social:
			if i in link:
				return True
		return False

	def crawl(self,pages,depth=2):
		global posts
		global blocked
		for i in range(depth):
			# time.sleep(5)
			new_pages = set()
			for page in pages:
				try:
					c = urllib2.urlopen(page)
					contents = c.read()
				except:
					continue
				# print "yes"
				try:
					soup = BeautifulSoup(contents, 'html.parser')
					data = soup.findAll(text=True)
					result = filter(self.clean_html,data)
					clean_content = self.remove_nonalphanum(result).encode('utf-8')
					# print clean_content
				except:
					continue
				if self.social_link(page):
					continue
				links = soup('a')
				graph_links = []
				for link in links:
					if "href" in link.attrs:
						link_string = link.attrs['href'].encode('utf-8').strip()
						if self.social_link(link):
							continue
						if link_string.startswith("http") and "uci" in link_string and "png" not in link_string and"jpg" not in link_string and "pdf" not in link_string:
							# addToIndex(link_string)
							if link_string not in dic and blocked[0] not in link_string and blocked[1] not in link_string:
								link_data = {
									'links_covered': link_string
								}
								graph_links.append(link_string)
								db.links.insert_one(link_data)
								new_pages.add(link_string)
								dic[link_string] = 1
				if page not in pages_indexed:
					pages_indexed[page] = 1
					post_data = {
						'link': page,
						'content': clean_content,
						'graph_links': graph_links
					}
					posts.insert_one(post_data)
			pages = list(new_pages)

	def get_docs(self):
		global posts
		cursor = posts.find({})
		contents = []
		for document in cursor:
			contents.append(document)

		return contents

	def get_doc_contents(self, full_doc_contents):
		contents = []
		for content in full_doc_contents:
			contents.append(content['content'])
		return contents



	def tokenize_words(self):
		global list_of_words
		full_doc_contents = self.get_docs()
		contents = self.get_doc_contents(full_doc_contents)		
		word_dictionary = {}
		word_id = 1
		for content in contents:
			splitter = re.compile('\\W*')
			words_list = [s.lower() for s in splitter.split(content) if s!= '']
			for word in words_list:
				if word not in word_dictionary and word not in stopwords:
					word_data = {
						'word_id': word_id,
						'word': word,
					}		
					list_of_words.insert_one(word_data)
					word_dictionary[word] = 1
					word_id += 1

	def findWholeWord(self,w):
	    return re.compile(r'\b({0})\b'.format(w), flags=re.IGNORECASE).search

	def word_document_relation(self, start, end, output):
		t = 1
		global list_of_words
		full_doc_contents = self.get_docs()
		contents = self.get_doc_contents(full_doc_contents)
		listed = list_of_words.find({})
		l_of_words = []
		for document in listed:
			l_of_words.append(document)

		start = 0
		end = len(l_of_words)
		for w in range(start,end):
			doc_list = []
			for content in full_doc_contents:
				if self.findWholeWord(l_of_words[w]['word'])(content['content']):
					doc_list.append(content['link'])
			l_of_words[w]['doc_list'] = doc_list
			list_of_words.update_one({'_id': l_of_words[w]['_id']},{"$set": l_of_words[w]}, upsert=False)


	def term_frequency(self, word, link):
		global posts
		document_of_interest = posts.find_one({'link':link})
		splitter = re.compile('\\W*')
		words_list = [s.lower() for s in splitter.split(document_of_interest['content']) if s!= '']

		tf = 0.0
		count = 0.0
		for w in words_list:
			if word == w:
				count += 1.0
		tf = float(float(count)/float(len(words_list)))
		return tf

	def id_frequency(self, word, num_of_docs_of_word):
		full_doc_contents = self.get_docs()
		contents = self.get_doc_contents(full_doc_contents)
		N = len(contents)
		idf = float(math.log(N/num_of_docs_of_word,10))
		return idf

	def fill_tfidf(self, start, end, output):
		l_of_words = []
		global list_of_words
		listed = list_of_words.find({})
		for document in listed:
			l_of_words.append(document['word'])	

		t = 0
		start = 0
		end = len(l_of_words)
		for w in range(start, end):
			word_doc = list_of_words.find_one({'word':l_of_words[w]})
			doc_list = word_doc['doc_list']
			idf = self.id_frequency(l_of_words[w], len(doc_list))
			scores = []
			for link in doc_list:
				tf = self.term_frequency(l_of_words[w], link)
				score = float(tf * idf)
				scores.append((score, link))
			scores = sorted(scores, reverse=True)
			word_doc['scores'] = scores
			list_of_words.update_one({'word':l_of_words[w]},{"$set": word_doc}, upsert=False)
		output.put(1)

	def setPageScore(self):
		global posts
		all_posts = posts.find({})
		all_links = []
		for docs in all_posts:
			all_links.append(docs['link'])

		for link in all_links:
			current_doc = posts.find_one({'link': link})
			current_doc['page_score'] = 0.15
			posts.update_one({'link': link},{"$set":current_doc}, upsert=False)


	def pageRank(self):
		global posts
		all_posts = posts.find({})
		all_links = []
		for docs in all_posts:
			all_links.append(docs['link'])

		self.setPageScore()

		for iterations in range(0,3):
			l = 0
			print "iteration " + str(iterations) + " .... "
			for link in all_links:
				# if l == 1:
				# 	break
				# l += 1
				current_doc = posts.find_one({'link': link})
				graph_links = current_doc['graph_links']
				link_page_rank_score = current_doc['page_score']
				for graph_link in graph_links:
					graph_doc = posts.find_one({'link': graph_link})
					if graph_doc:
						num_of_g_links = len(graph_doc['graph_links'])
						g_link_pr_score = graph_doc['page_score']
						# print num_of_g_links, g_link_pr_score
						if num_of_g_links != 0:
							# print num_of_g_links, g_link_pr_score
							link_page_rank_score = 0.85*(float(g_link_pr_score)/float(num_of_g_links+1))
				link_page_rank_score += 0.15
				current_doc['page_score'] = link_page_rank_score
				posts.update_one({'link':link},{"$set": current_doc}, upsert=False)

blocked = ['mlearn', 'calendar']
social = ['facebook', 'twitter','soundcloud']
# crawl(["https://www.ics.uci.edu"])
# output = mp.Queue()
# tokenize_words()
# word_document_relation()
# start = 0
# end = 27850
# processes = []
# for _ in range(2):
# 	process = mp.Process(target=fill_tfidf,args=(start,end,output))
# 	start += 27850
# 	end += 27850
# 	processes.append(process)

# for p in processes:
# 	p.start()

# for p in processes:
# 	p.join()


# pageRank()
# setPageScore()

