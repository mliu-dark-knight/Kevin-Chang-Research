
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


Paper = namedtuple("Paper", "title, year, platform")
AuthorOf = namedtuple("AuthorOf", "name, title")

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


papers = []
authorof = []

def parse():
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
					if str(t.text):
						title = t.text
						break

				for y in elem.findall('year'):
					if str(y.text):
						year = y.text
						break

				for p in elem.findall('journal') or elem.findall('booktitle'):
					if str(p.text):
						platform = p.text
						break

				papers.append(Paper(title, year, platform))

				author = []
				for a in elem.findall('author'):
					if str(a.text):
					 	author.append(a.text)
					 	authorof.append(AuthorOf(a.text, title))
				w.writerow(author)

			elem.clear()
	f.close()


def to_csv():
	df = pd.DataFrame(papers, columns=["title", "year", "platform"])
	df.to_csv("Paper.csv", index=False, encoding='utf-8')

	df = pd.DataFrame(authorof, columns=["name", "title"])
	df.to_csv("AuthorOf.csv", index=False, encoding='utf-8')



parse()
to_csv()


