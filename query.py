import searchengine
import pymongo
from pymongo import MongoClient
import time
import multiprocessing as mp 

output = mp.Queue()

client = MongoClient('mongodb://localhost:27017')

db = client['final_search_engine_cs']

posts = db.posts
links = db.links
list_of_words = db.words

my_engine = searchengine.searchEngine()

print "Website is being crawled ..."
#entire website to be crawled (as a list) and depth of crawling
site_to_be_crawled = ['https://www.csc.ncsu.edu/']
depth = 15
my_engine.crawl(site_to_be_crawled)

print "Done crawling the uci website .... "

#once all information has been gathered, tokenize the words
print "Tokenizing the words... "
my_engine.tokenize_words()

print "Done tokenizing the words ..."

#word_document relationship
print "Building a relationship between words and documents... "
my_engine.word_document_relation(0,10,output)

print "Built the word document relationship... "

#calculate tf-idf scores
print "Calculating the tfidf score... "
my_engine.fill_tfidf(0,10,output)
print "Done with the tfidf score... "

#page_rank
print "Page importance with pagerank... "
my_engine.setPageScore()
my_engine.pageRank()
print "Done with everything, ready to query? "


#Querying

query = raw_input("Enter your search string... ")

start = time.time()
links = list_of_words.find_one({'word':query})

if links:
	i = 0

	for link in links['doc_list']:
		if i == 20:
			break
		i += 1
		print link

	end = time.time()

	print "returned result in " + str(end-start) + " ...."

else:
	print "Sorry such a result could not be found in NCSU domain ..."
	end = time.time()
	print "returned result in " + str(end-start) + " ...."
	
