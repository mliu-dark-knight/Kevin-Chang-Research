import xml.dom.minidom
import pandas as pd
from collections import namedtuple
from xml.dom.minidom import parse


AuthorOf = namedtuple("AuthorOf", "title, author")
Paper = namedtuple("Paper", "title, year, platform")


class XML_Parser(object):
   def __init__(self):
      self.papers = []
      self.authorOf = []

   def platform_tags(self):
      return ["booktitle", "journal", "school"]
     

   def parse(self, fname):
      DOMTree = xml.dom.minidom.parse(fname)
      collection = DOMTree.documentElement

      papers = set()
      for tag in ["article", "incollection", "inproceedings", "mastersthesis", "phdthesis"]:
         papers |= set(collection.getElementsByTagName(tag))

      for paper in papers:
         print "*****PAPER*****"

         '''
         if paper.hasAttribute("mdate"):
            print "mdate: %s" % paper.getAttribute("mdate")

         if paper.hasAttribute("key"):
            print "key: %s" % paper.getAttribute("key")
         '''

         title = paper.getElementsByTagName("title")[0].childNodes[0].data
         print "title: %s" % title

         year = paper.getElementsByTagName("year")[0].childNodes[0].data
         print "year: %s" % year

         for tag in self.platform_tags():
            if len(paper.getElementsByTagName(tag)) != 0:
               platform = paper.getElementsByTagName(tag)[0].childNodes[0].data
               break
         print "platform: %s" % platform

         self.papers.append(Paper(title, year, platform))

         authors = paper.getElementsByTagName("author")
         for author in authors:
            print "author: %s" % author.childNodes[0].data
            self.authorOf.append(AuthorOf(title, author.childNodes[0].data))

   def to_csv(self):
      df = pd.DataFrame(self.papers, columns=["title", "year", "platform"])
      df.to_csv("Paper.csv", index=False)

      df = pd.DataFrame(self.authorOf, columns=["title", "author"])
      df.to_csv("AuthorOf.csv", index=False)


parser = XML_Parser()
parser.parse("dblp.xml")
parser.to_csv()
