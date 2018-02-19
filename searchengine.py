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

db = client['final_search_engine_cs']

posts = db.posts
links = db.links
list_of_words = db.words

# def build_graph():


stopwords = list(set(stopwords.words('english')))

dic = {}
pages_indexed = {}
unwanted = ['/', '\\', '@', '+', ':','#']

# class searchEngine:

def clean_html(element):
	# print "yes"
	if element.parent.name in ['style', 'script', '[document]', 'head', 'title']:
		return False
	elif re.match('<!--.*-->', str(element.encode('utf-8'))):
		return False
	else:
		return True

def remove_nonalphanum(result):
	for w in range(0,len(result)):
		for i in unwanted:
			if i in result[w]:
				# print "yes", w
				result[w] = " "

	clean_text = ' '.join(result).replace("\n", " ")
	return clean_text

def social_link(link):
	for i in social:
		if i in link:
			return True
	return False

def crawl(pages,depth=12):
	for i in range(depth):
		# time.sleep(5)
		new_pages = set()
		for page in pages:
			try:
				# print "in"
				c = urllib2.urlopen(page)
				contents = c.read()
			except:
				continue
			# print "yes"
			try:
				soup = BeautifulSoup(contents, 'html.parser')
				data = soup.findAll(text=True)
				result = filter(clean_html,data)
				clean_content = remove_nonalphanum(result).encode('utf-8')
				# print clean_content
			except:
				continue
			if social_link(page):
				continue
			links = soup('a')
			graph_links = []
			for link in links:
				if "href" in link.attrs:
					link_string = link.attrs['href'].encode('utf-8').strip()
					if social_link(link):
						continue
					if link_string.startswith("http") and "uci" in link_string and "pdf" not in link_string:
						# addToIndex(link_string)
						if link_string not in dic and blocked[0] not in link_string and blocked[1] not in link_string:
							link_data = {
								'links_covered': link_string
							}
							graph_links.append(link_string)
							db.links.insert_one(link_data)
							new_pages.add(link_string)
							dic[link_string] = 1
			if page in dic1:
				p_data = posts.find_one({'link':page})
				p_data['graph_links'] = graph_links
				posts.update_one({'_id': p_data['_id']},{"$set": p_data}, upsert=False)
				continue

			if page not in pages_indexed:
				pages_indexed[page] = 1
				post_data = {
					'link': page,
					'content': clean_content,
					'graph_links': graph_links
				}
				posts.insert_one(post_data)
		pages = list(new_pages)

def get_docs():
	cursor = posts.find({})
	contents = []
	for document in cursor:
		contents.append(document)

	return contents

full_doc_contents = get_docs()

def get_doc_contents():
	contents = []
	for content in full_doc_contents:
		contents.append(content['content'])
	return contents

contents = get_doc_contents()

def tokenize_words():
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

def findWholeWord(w):
    return re.compile(r'\b({0})\b'.format(w), flags=re.IGNORECASE).search

def word_document_relation(start, end, output):
	t = 1
	listed = list_of_words.find({})
	l_of_words = []
	for document in listed:
		l_of_words.append(document)


	for w in range(start,end):
		doc_list = []
		for content in full_doc_contents:
			if findWholeWord(l_of_words[w]['word'])(content['content']):
				doc_list.append(content['link'])
		print t
		t += 1
		l_of_words[w]['doc_list'] = doc_list
		list_of_words.update_one({'_id': l_of_words[w]['_id']},{"$set": l_of_words[w]}, upsert=False)

	# for word in l_of_words:
	# 	doc_list = []
	# 	for content in full_doc_contents:
	# 		# splitter = re.compile('\\W*')
	# 		# words_list = [s.lower() for s in splitter.split(content['content']) if s!= '']
	# 		# if word['word'] in words_list:
	# 		# 	doc_list.append(content['link'])
	# 		if findWholeWord(word['word'])(content['content']):
	# 			doc_list.append(content['link'])
	# 	print t
	# 	t += 1
	# 	word['doc_list'] = doc_list
	# 	list_of_words.update_one({'_id': word['_id']},{'$set':word}, upsert=False)

def term_frequency(word, link):
	document_of_interest = posts.find_one({'link':link})
	splitter = re.compile('\\W*')
	words_list = [s.lower() for s in splitter.split(document_of_interest['content']) if s!= '']

	tf = 0.0
	count = 0.0
	for w in words_list:
		if word == w:
			count += 1.0
	# if count == 0.0:
	# 	print word, count, link
	tf = float(float(count)/float(len(words_list)))
	return tf

def id_frequency(word, num_of_docs_of_word):
	N = len(contents)
	# count = 0
	# for content in cts:
	# 	if word in content:
	# 		count += 1
	# print num_of_docs_of_word
	idf = float(math.log(N/num_of_docs_of_word,10))
	return idf

def fill_tfidf(start, end, output):
	l_of_words = []
	listed = list_of_words.find({})
	for document in listed:
		l_of_words.append(document['word'])	

	t = 0
	for w in range(start, end):
		word_doc = list_of_words.find_one({'word':l_of_words[w]})
		doc_list = word_doc['doc_list']
		idf = id_frequency(l_of_words[w], len(doc_list))
		scores = []
		for link in doc_list:
			tf = term_frequency(l_of_words[w], link)
			score = float(tf * idf)
			scores.append((score, link))
		print t
		t += 1
		scores = sorted(scores, reverse=True)
		word_doc['scores'] = scores
		list_of_words.update_one({'word':l_of_words[w]},{"$set": word_doc}, upsert=False)
	# for w in l_of_words:
	# 	print b
	# 	b += 1
	# 	word_doc = list_of_words.find_one({'word':w})
	# 	print word_doc['word']
	# 	# print word_doc['doc_list']
	# 	doc_list = word_doc['doc_list']
	# 	idf = id_frequency(w, len(doc_list))
	# 	scores = []
	# 	for link in doc_list:
	# 		tf = term_frequency(w,link)
	# 		score = float(tf * idf)
	# 		scores.append(score)
	# 	scores = sorted(scores)[::-1]
	# 	word_doc['scores'] = scores
	# 	list_of_words.update_one({'word': w},{"$set":word_doc}, upsert=False)
	output.put(1)

def setPageScore():
	all_posts = posts.find({})
	all_links = []
	for docs in all_posts:
		all_links.append(docs['link'])

	for link in all_links:
		current_doc = posts.find_one({'link': link})
		current_doc['page_score'] = 1.0
		posts.update_one({'link': link},{"$set":current_doc}, upsert=False)


def pageRank():
	all_posts = posts.find({})
	all_links = []
	for docs in all_posts:
		all_links.append(docs['link'])

	setPageScore()

	for iterations in range(0,3):
		l = 0
		for link in all_links:
			if l == 1:
				break
			l += 1
			current_doc = posts.find_one({'link': link})
			graph_links = current_doc['graph_links']
			link_page_rank_score = current_doc['page_score']
			for graph_link in graph_links:
				graph_doc = posts.find_one({'link': graph_link})
				if graph_doc:
					num_of_g_links = len(graph_doc['graph_links'])
					g_link_pr_score = graph_doc['page_score']
					print num_of_g_links, g_link_pr_score
					link_page_rank_score += 0.85*(float(g_link_pr_score)/float(num_of_g_links+1))
			# link_page_rank_score += 0.15
			current_doc['page_score'] = link_page_rank_score
			posts.update_one({'link':link},{"$set": current_doc}, upsert=False)

blocked = ['mlearn', 'calendar']
social = ['facebook', 'twitter','soundcloud']
# crawl(["https://www.ics.uci.edu"])
output = mp.Queue()
# tokenize_words()
word_document_relation()
start = 0
end = 27850
processes = []
for _ in range(2):
	process = mp.Process(target=fill_tfidf,args=(start,end,output))
	start += 27850
	end += 27850
	processes.append(process)

for p in processes:
	p.start()

for p in processes:
	p.join()


pageRank()
setPageScore()

