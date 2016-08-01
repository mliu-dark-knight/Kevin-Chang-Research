import numpy as np
import networkx as nx
import matplotlib.pyplot as plt
from abc import ABCMeta, abstractmethod
from scipy.sparse import csc_matrix
from neo4j.v1 import GraphDatabase, basic_auth


class recommend(object):
	__metaclass__ = ABCMeta

	def __init__(self, session):
		self.personalization = {}
		self.startID = -1
		self.G = nx.Graph()
		self.candidates = {}
		self.threshold = 0.25
		self.session = session

	@abstractmethod
	def setStart(self, input):
		pass

	@abstractmethod
	def constructGraph(self):
		pass

	@abstractmethod
	def generateCandidates(self):
		pass

	def getProperty(self, candidateID):
		return self.candidates[candidateID]

	def recommend(self, input, step):
		self.setStart(input)
		assert self.startID > -1
		assert not self.personalization
		assert not self.candidates
		self.personalization[self.startID] = 1.0
		self.constructGraph()
		self.generateCandidates()
		# self.visualize()
		self.rank = nx.pagerank_scipy(self.G, alpha = 0.9, personalization = self.personalization, tol = 1e-8, max_iter = 256)
		# self.showRank()
		return self.filterResult()

	def filterResult(self):
		recommendationList = []
		for k, v in self.rank.iteritems():
			shouldRecommend, prop, pgr = self.getProperty(k, v)
			if shouldRecommend:
				recommendationList.append((prop, pgr))
		return recommendationList

	def showRank(self):
		for k, v in self.rank.iteritems():
			print k, v

	def visualize(self):
		plt.show(nx.draw(self.G, nodecolor = 'r',edge_color = 'b'))
		


class pprPaperToResearcher(recommend):
	def setStart(self, input):
		self.startID = getResearcherByName(input, self.session)

	def generateCandidates(self):
		papers = list(self.session.run("match (r:Researcher)-[*1..3]-(p:Paper) where ID(r) = %d and not (r)-[:AuthorOf]-(p) return ID(p) as ID, p.title as title, p.pagerank as PR" % self.startID))
		for paper in papers:
			self.candidates[paper["ID"]] = (paper["title"], paper["PR"])
		

class pprResearcherToPaper(recommend):
	def setStart(self, input):
		self.startID = getPaperByTitle(input, self.session)

	def generateCandidates(self):
		researchers = list(self.session.run("match (p:Paper)-[*1..3]-(r:Researcher) where ID(p) = %d and not (p)-[:AuthorOf]-(p) return ID(r) as ID, r.name as name, r.pagerank as PR" % self.startID))
		for researcher in researchers:
			self.candidates[researcher["ID"]] = (researcher["name"], researcher["PR"])


class pprResearcherToResearcher(recommend):
	def setStart(self, input):
		self.startID = getResearcherByName(input, self.session)


class pprPaperToPaper(recommend):
	def setStart(self, input):
		self.startID = getPaperByTitle(input, self.session)



def getResearcherByName(name, session):
	return session.run("match (r:Researcher {name:'%s'}) return ID(r) as ID" % name).single()["ID"]


def getPaperByTitle(title, session):
	return session.run("match (p:Paper {title: '%s'}) return ID(p) as ID" % title).single()["ID"]


