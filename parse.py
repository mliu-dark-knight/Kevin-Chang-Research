
# coding: utf-8

from xml.etree.ElementTree import iterparse, XMLParser
from collections import namedtuple
import xml.etree.cElementTree as ET
import pandas as pd
import htmlentitydefs
import csv
import sys


reload(sys)
sys.setdefaultencoding('utf-8')


Paper = namedtuple("Paper", "paperID, title, year, pagerank, label")
Researcher = namedtuple("Researcher", "name, pagerank, label")
Platform = namedtuple("Platform", "platform, pagerank, label")
AuthorOf = namedtuple("AuthorOf", "name, paperID, type")
WrittenBy = namedtuple("WrittenBy", "paperID, name, type")
PublishAt = namedtuple("PublishAt", "paperID, platform, type")
Publishes = namedtuple("Publishes", "platform, paperID, type")


papers = []
authorof = []
writtenby = []
publishat = []
publishes = []
researchers = []
platforms = []
dict_researcher = set()
dict_platform = set()


class CustomEntity:
	def __getitem__(self, key):
		if key == 'umml':
			key = 'uuml' # Fix invalid entity
		return unichr(htmlentitydefs.name2codepoint[key])
		

parser = XMLParser()
parser.parser.UseForeignDTD(True)
parser.entity = CustomEntity()


def paper_tags():
	return set(["article", "incollection", "inproceedings"])


def strip_comma(input):
	return input.strip().replace(', ', '').replace(',', ' ')

def valid_text(text):
	return text != None

def valid_title(text):
	return valid_text(text) and len(text) >= 16

def valid_name(text):
	return valid_text(text) and len(text) >= 4

def valid_platform(text):
	return valid_text(text) and len(text) >= 2

def normalize(text):
	assert valid_text(text)
	if text[0] != '"' and text[-1] != '"':
		return '"' + text + '"'
	return text



def parse():
	paper_id = 0

	with open("CoAuthor.csv", "w") as f:
		w = csv.writer(f)
		w.writerow(["names"])
		for event, elem in iterparse('dblp.xml', events=['start'], parser=parser):
			if elem.tag in paper_tags():
				title = ""
				year = -1
				platform = ""

				# print "****Paper****"
				for t in elem.findall('title'):
					if valid_title(t.text):
						title = strip_comma(t.text)
						break

				for y in elem.findall('year'):
					if valid_text(y.text):
						year = y.text
						break

				for p in elem.findall('journal') or elem.findall('booktitle'):
					if valid_platform(p.text):
						platform = strip_comma(p.text)

						if platform not in dict_platform:
							dict_platform.add(platform)
						
						if valid_title(title):
							publishat.append(PublishAt(paper_id, platform, "PublishAt"))
							# publishes.append(PublishAt(platform, paper_id, "Publishes"))
					
						break

				authors = []
				for a in elem.findall('author'):
					if valid_name(a.text):
						author = a.text
						if author not in dict_researcher:
							dict_researcher.add(author)
					 	authors.append(author)

					 	if valid_title(title):
					 		authorof.append(AuthorOf(author, paper_id, "AuthorOf"))
					 		# writtenby.append(WrittenBy(paper_id, author, "WrittenBy"))

				w.writerow(authors)

				if valid_title(title):
					papers.append(Paper(paper_id, title, year, 0.0, "Paper"))
					paper_id += 1

			elem.clear()
	f.close()


'''
Format required by neo4j
Do not change orders, node should be inserted before inserting edges
'''
def to_csv():
	df = pd.DataFrame(papers, columns = ["paperID:ID", "title", "year", "pagerank", ":LABEL"])
	df.to_csv("Paper.csv", index = False, encoding = 'utf-8')

	researchers = [Researcher(name, 0.0, "Researcher") for name in dict_researcher]	
	df = pd.DataFrame(researchers, columns = ["name:ID", "pagerank", ":LABEL"])
	df.to_csv("Researcher.csv", index = False, encoding = 'utf-8')

	platforms = [Platform(platform, 0.0, "Platform") for platform in dict_platform]	
	df = pd.DataFrame(platforms, columns = ["platform:ID", "pagerank", ":LABEL"])
	df.to_csv("Platform.csv", index = False, encoding = 'utf-8')

	df = pd.DataFrame(authorof, columns = [":START_ID", ":END_ID", ":TYPE"])
	df.to_csv("AuthorOf.csv", index = False, encoding = 'utf-8')

	# df = pd.DataFrame(writtenby, columns = [":START_ID", ":END_ID", ":TYPE"])
	# df.to_csv("WrittenBy.csv", index = False, encoding = 'utf-8')

	df = pd.DataFrame(publishat, columns = [":START_ID", ":END_ID", ":TYPE"])
	df.to_csv("PublishAt.csv", index = False, encoding = 'utf-8')

	# df = pd.DataFrame(publishes, columns = [":START_ID", ":END_ID", ":TYPE"])
	# df.to_csv("Publishes.csv", index = False, encoding = 'utf-8')



parse()
to_csv()


