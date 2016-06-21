import xml.dom.minidom
import codecs
import re
import pandas as pd
from collections import namedtuple
from xml.dom.minidom import parse
import xml.etree.ElementTree as etree


AuthorOf = namedtuple("AuthorOf", "title, author")
Paper = namedtuple("Paper", "title, year, platform")
Thesis = namedtuple("Thesis", "title, year, school")
Book = namedtuple("Book", "title, year")


class XML_Parser(object):
	def __init__(self):
		self.papers = []
		self.thesis = []
		self.books = []
		self.authorOf = []

	def platform_tags(self):
		return ["booktitle", "journal"]

	def school_tags(self):
		return ["school"]

	def paper_tags(self):
		return set(["article", "incollection", "inproceedings"])

	def thesis_tags(self):
		return set(["mastersthesis", "phdthesis"])

	def book_tags(self):
		return set(["book"])

	def pthb_tags(self):
		return self.paper_tags() | self.thesis_tags() | self.book_tags()


	def remove_invalid_char(self, input, output):
		invalid_xmls = [re.compile(r'&\w\w;'), re.compile(r'&\w\w\w;'), 
						re.compile(r'&\w\w\w\w;'), re.compile(r'&\w\w\w\w\w;'), 
						re.compile(r'&\w\w\w\w\w\w;'), re.compile(r'&\w\w\w\w\w\w\w;'), 
						re.compile(r'<i>'), re.compile(r'</i>'), 
						re.compile(r'<sub>'), re.compile(r'</sub>'), 
						re.compile(r'<sup>'), re.compile(r'</sup>')]
		conn = open(input, 'r')

		new_file = open(output,'w') 
		with open(input) as f:
			for line in f:
				nline = line
				for invalid_xml in invalid_xmls:
					nline, count = invalid_xml.subn('', nline)
				new_file.write(nline) 
		new_file.close()


	def iter_parse(self, fname):
		events = ('start', 'end', 'start-ns', 'end-ns')
		authors = []

		for event, elem in etree.iterparse(fname, events=events):

			if event == "start":
				if elem.tag == "book":
					skip = True

				if elem.tag in self.pthb_tags():
					# print "*****PAPER/Thesis/Book*****"
					pass

				elif elem.tag == "title":
					title = elem.text
					# print "title: %s" % title

				elif elem.tag == "year":
					year = elem.text
					# print "year: %s" % year

				elif elem.tag in self.platform_tags():
					platform = elem.text
					# print "platform: %s" % platform

				elif elem.tag in self.school_tags():
					school = elem.text
					# print "school: %s" % school

				elif elem.tag == "author":
					author = elem.text
					authors.append(author)
					# print "author: %s" % author

			if event == "end":
				if elem.tag in self.pthb_tags():
					if elem.tag in self.paper_tags():
						self.papers.append(Paper(title, year, platform))

					elif elem.tag in self.thesis_tags():
						self.thesis.append(Thesis(title, year, school))

					elif elem.tag in self.book_tags():
						self.books.append(Book(title, year))

					for author in authors:
						self.authorOf.append(AuthorOf(title, author))

					title = ""
					year = ""
					platform = ""
					school = ""
					authors = []
					elem.clear()

	 

	def to_csv(self):
		df = pd.DataFrame(self.papers, columns=["title", "year", "platform"])
		df.to_csv("Paper.csv", index=False, encoding='utf-8')

		df = pd.DataFrame(self.thesis, columns=["title", "year", "school"])
		df.to_csv("Thesis.csv", index=False, encoding='utf-8')

		df = pd.DataFrame(self.books, columns=["title", "year"])
		df.to_csv("Book.csv", index=False, encoding='utf-8')

		df = pd.DataFrame(self.authorOf, columns=["title", "author"])
		df.to_csv("AuthorOf.csv", index=False, encoding='utf-8')


parser = XML_Parser()
# parser.remove_invalid_char("dblp.xml", "dblp_corrected.xml")
parser.iter_parse("dblp_corrected.xml")
parser.to_csv()
