from xml.etree.ElementTree import iterparse, XMLParser
from collections import namedtuple
import xml.etree.cElementTree as ET
import pandas as pd
import htmlentitydefs
import csv
import sys
import re
import numpy as np


reload(sys)
sys.setdefaultencoding('utf-8')


Paper = namedtuple("Paper", "paperID, title, year, label")
Researcher = namedtuple("Researcher", "name, label")
Conference = namedtuple("Conference", "conference, label")
AuthorOf = namedtuple("AuthorOf", "name, weight, paperID, type")
PublishAt = namedtuple("PublishAt", "paperID, weight, conference, type")
Reference = namedtuple("Reference", "paperID, weight, sourceID, type")


papers = []
researchers = []
conferences = []
authorof = []
publishat = []
reference_buffer = []
reference = []
dict_researcher = set()
dict_conference = set()
dict_paper = set()
dict_badpaper = set()
dict_reference = set()


def strip_comma(input):
	return input.strip().replace(',', '').replace('\"', '')

def weight(year):
	return np.exp((int(year) - 2016) * 1e-2)


def parse():
	paperID = None
	title = None
	year = None
	authors = None
	conference = None
	citeID = None

	newpaperID = int('400000000000000000000000', 16)

	with open("../data/dblp.txt", "r") as f:
		for line in f:
			line = line[:-1]
			if line.startswith('#*'):
				title = strip_comma(line[2:])
				dict_reference = set() 


			elif line.startswith('#@'):
				authors = line[2:].split(', ')
				for author in authors:
					if author not in dict_researcher:
						dict_researcher.add(author)
						researchers.append(Researcher(author, "Researcher"))

			elif line.startswith('#t'):
				year = line[2:]

			elif line.startswith('#c'):
				conference = line[2:]
				if conference not in dict_conference:
					dict_conference.add(conference)
					if conference != "CoRR":
						conferences.append(Conference(conference, "Conference"))

			elif line.startswith('#index'):
				paperID = line[6:]

				if paperID in dict_paper:
					paperID = format(newpaperID, 'x')
					newpaperID += 1

				if ("Proceedings" in title or "Conference" in title) and re.compile("[0-9]+").search(title) != None:
					dict_badpaper.add(paperID)
					title = None
					year = None
					authors = None
					conference = None
					citeID = None
					continue

				dict_paper.add(paperID)
				papers.append(Paper(paperID, title, year, "Paper"))

				if conference != None and conference != "CoRR":
					publishat.append(PublishAt(paperID, weight(year), conference, "PublishAt"))

				if authors != None:
					for author in authors:
						authorof.append(AuthorOf(author, weight(year), paperID, "AuthorOf"))

				title = None
				year = None
				authors = None
				conference = None
				citeID = None


			elif line.startswith('#%'):
				citeID = line[2:]
				if citeID not in dict_reference:
					dict_reference.add(citeID)
					reference_buffer.append((paperID, citeID))

	f.close()

	for (paperID, citeID) in reference_buffer:
		if paperID not in dict_badpaper and citeID not in dict_badpaper:
			reference.append(Reference(paperID, 0.9, citeID, "Reference"))


'''
Format required by neo4j
Do not change orders, node should be inserted before inserting edges
'''
def to_csv():
	df = pd.DataFrame(papers, columns = ["paperID:ID", "title", "year:INT", ":LABEL"])
	df.to_csv("../data/Paper.csv", index = False, encoding = 'utf-8')

	df = pd.DataFrame(researchers, columns = ["name:ID", ":LABEL"])
	df.to_csv("../data/Researcher.csv", index = False, encoding = 'utf-8')

	df = pd.DataFrame(conferences, columns = ["conference:ID", ":LABEL"])
	df.to_csv("../data/Conference.csv", index = False, encoding = 'utf-8')

	df = pd.DataFrame(authorof, columns = [":START_ID", "weight:FLOAT", ":END_ID", ":TYPE"])
	df.to_csv("../data/AuthorOf.csv", index = False, encoding = 'utf-8')

	df = pd.DataFrame(publishat, columns = [":START_ID", "weight:FLOAT", ":END_ID", ":TYPE"])
	df.to_csv("../data/PublishAt.csv", index = False, encoding = 'utf-8')

	df = pd.DataFrame(reference, columns = [":START_ID", "weight:FLOAT", ":END_ID", ":TYPE"])
	df.to_csv("../data/Reference.csv", index = False, encoding = 'utf-8')


parse()
to_csv()
