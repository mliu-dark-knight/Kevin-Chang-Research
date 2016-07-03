
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


Paper = namedtuple("Paper", "title, year, platform, label")
Researcher = namedtuple("Researcher", "name, label")
Platform = namedtuple("Platform", "platform, label")
AuthorOf = namedtuple("AuthorOf", "name, title, type")
WrittenBy = namedtuple("WrittenBy", "title, name, type")
PublishAt = namedtuple("PublishAt", "title, platform, type")
Publishes = namedtuple("Publishes", "platform, title, type")

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


def strip_quotation(input):
	return input.strip().replace('"', '')



papers = []
authorof = []
writtenby = []
publishat = []
publishes = []
researchers = []
platforms = []
dict_researcher = set()
dict_platform = set()


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
					if str(p.text) and p.text != "":
						platform = p.text

						if title != "":
							publishat.append(PublishAt(title, platform, "PublishAt"))
							publishes.append(PublishAt(platform, title, "Publishes"))

						if platform not in dict_platform:
							dict_platform.add(platform)
						break

				if title != "":
					papers.append(Paper(title, year, platform, "Paper"))

				author = []
				for a in elem.findall('author'):
					if str(a.text) != "":
						if a.text not in dict_researcher:
							dict_researcher.add(a.text)
					 	author.append(a.text)

					 	if title != "":
					 		authorof.append(AuthorOf(a.text, title, "AuthorOf"))
					 		writtenby.append(WrittenBy(title, a.text, "WrittenBy"))

				w.writerow(author)
			elem.clear()
	f.close()


'''
Format required by neo4j
Do not change orders, node should be inserted before inserting edges
'''
def to_csv():
	df = pd.DataFrame(papers, columns = ["title:ID", "year", "platform", ":LABEL"])
	df.to_csv("Paper.csv", index = False, encoding = 'utf-8')

	researchers = [Researcher(name, "Researcher") for name in dict_researcher]	
	df = pd.DataFrame(researchers, columns = ["name:ID", ":LABEL"])
	df.to_csv("Researcher.csv", index = False, encoding = 'utf-8')

	platforms = [Platform(platform, "Platform") for platform in dict_platform]	
	df = pd.DataFrame(platforms, columns = ["platform:ID", ":LABEL"])
	df.to_csv("Platform.csv", index = False, encoding = 'utf-8')

	df = pd.DataFrame(authorof, columns = [":START_ID", ":END_ID", ":TYPE"])
	df.to_csv("AuthorOf.csv", index = False, encoding = 'utf-8')

	df = pd.DataFrame(writtenby, columns = [":START_ID", ":END_ID", ":TYPE"])
	df.to_csv("WrittenBy.csv", index = False, encoding = 'utf-8')

	df = pd.DataFrame(publishat, columns = [":START_ID", ":END_ID", ":TYPE"])
	df.to_csv("PublishAt.csv", index = False, encoding = 'utf-8')

	df = pd.DataFrame(publishes, columns = [":START_ID", ":END_ID", ":TYPE"])
	df.to_csv("Publishes.csv", index = False, encoding = 'utf-8')



parse()
to_csv()


