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
Conference = namedtuple("Conference", "conference, pagerank, label")
AuthorOf = namedtuple("AuthorOf", "name, paperID, type")
PublishAt = namedtuple("PublishAt", "paperID, conference, type")
Reference = namedtuple("Reference", "paperID, sourceID, type")


papers = []
researchers = []
conferences = []
authorof = []
publishat = []
reference = []
dict_researcher = set()
dict_conference = set()
dict_paper = set()


def strip_comma(input):
	return input.strip().replace(',', '').replace('\"', '')


def parse():
	paperID = None
	title = None
	year = None
	authors = None
	conference = None
	citeID = None

	newpaperID = int('400000000000000000000000', 16)

	with open("dblp.txt", "r") as f:
		for line in f:
			line = line[:-1]
			if line.startswith('#*'):
				title = strip_comma(line[2:])

			elif line.startswith('#@'):
				authors = line[2:].split(', ')
				for author in authors:
					if author not in dict_researcher:
						dict_researcher.add(author)
						researchers.append(Researcher(author, 0, "Researcher"))

			elif line.startswith('#t'):
				year = line[2:]

			elif line.startswith('#c'):
				conference = line[2:]
				if conference not in dict_conference:
					dict_conference.add(conference)
					conferences.append(Conference(conference, 0, "Conference"))

			elif line.startswith('#index'):
				paperID = line[6:]

				if paperID in dict_paper:
					paperID = format(newpaperID, 'x')
					newpaperID += 1

				dict_paper.add(paperID)
				papers.append(Paper(paperID, title, year, 0, "Paper"))

				if conference != None:
					publishat.append(PublishAt(paperID, conference, "PublishAt"))

				if authors != None:
					for author in authors:
						authorof.append(AuthorOf(author, paperID, "Authorof"))

				title = None
				year = None
				authors = None
				conference = None
				citeID = None


			elif line.startswith('#%'):
				citeID = line[2:]
				reference.append(Reference(paperID, citeID, "Reference"))

	f.close()


'''
Format required by neo4j
Do not change orders, node should be inserted before inserting edges
'''
def to_csv():
	df = pd.DataFrame(papers, columns = ["paperID:ID", "title", "year", "pagerank", ":LABEL"])
	df.to_csv("Paper.csv", index = False, encoding = 'utf-8')

	researchers = [Researcher(name, 0, "Researcher") for name in dict_researcher]	
	df = pd.DataFrame(researchers, columns = ["name:ID", "pagerank", ":LABEL"])
	df.to_csv("Researcher.csv", index = False, encoding = 'utf-8')

	conferences = [Conference(conference, 0, "Conference") for conference in dict_conference]	
	df = pd.DataFrame(conferences, columns = ["conference:ID", "pagerank", ":LABEL"])
	df.to_csv("Conference.csv", index = False, encoding = 'utf-8')

	df = pd.DataFrame(authorof, columns = [":START_ID", ":END_ID", ":TYPE"])
	df.to_csv("AuthorOf.csv", index = False, encoding = 'utf-8')

	df = pd.DataFrame(publishat, columns = [":START_ID", ":END_ID", ":TYPE"])
	df.to_csv("PublishAt.csv", index = False, encoding = 'utf-8')

	df = pd.DataFrame(reference, columns = [":START_ID", ":END_ID", ":TYPE"])
	df.to_csv("Reference.csv", index = False, encoding = 'utf-8')


parse()
to_csv()
