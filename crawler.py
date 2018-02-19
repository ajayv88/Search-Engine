import urllib2
from bs4 import BeautifulSoup
from urlparse import urljoin
import pymongo
from pymongo import MongoClient
import time

client = MongoClient('mongodb://localhost:27017')

db = client['final_search_engine_cs']

posts = db.posts
links = db.links

cursor = posts.find({})
cursor1 = links.find({})

social = ['facebook', 'twitter','soundcloud']
blocked = ['mlearn', 'calendar']
def social_link(link):
	for i in social:
		if i in link:
			return True
	return False

dic = {}
dic2 = {}
j = 3653
arr1 = []
for document in cursor:
	if document['link'] not in dic:
		dic[document['link']] = 1
	arr1.append(document['link'])
# i = 0
arr = []
for document in cursor1:
	if document['links_covered'] not in dic2:
		dic2[document['links_covered']] = 1
		arr.append(document['links_covered'])

breaker = 0
# print arr1[3288]
# print len(arr1)
for i in range(3288,len(arr1)):
	if breaker == 4:
		break
	#if arr[i] not in dic:
		# print breaker
		# breaker += 1
	try:
		# print i
		# print arr[i]
		c = urllib2.urlopen(arr[i])
		contents = c.read()
		# print "in this"
	except:
		# print "in this"
		continue
	# print arr[i]
	try:
		print "in working"
		soup = BeautifulSoup(contents,'html.parser')
		html_content = soup.prettify().encode('utf-8').strip()
		links = soup('a')
		graph_links = []
		for link in links:
			if "href" in link.attrs:
				link_string = link.attrs['href'].encode('utf-8').strip()
				if social_link(link):
					continue
				if link_string.startswith("http") and "uci" in link_string and "pdf" not in link_string:
					graph_links.append(link_string)
				
				# 	# addToIndex(link_string)
				# 	if link_string not in dic and blocked[0] not in link_string and blocked[1] not in link_string:
				# 		# link_data = {
				# 		# 	'links_covered': link_string
				# 		# }
						
				# 		db.links.insert_one(link_data)
				# 		new_pages.add(link_string)
				# 		dic[link_string] = 1
		# if page in dic1:
		p_data = posts.find_one({'link':arr1[i]})
		p_data['graph_links'] = graph_links
		posts.update_one({'_id': p_data['_id']},{"$set": p_data}, upsert=False)
	except:
		continue