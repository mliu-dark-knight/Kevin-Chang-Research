import numpy as np
import networkx as nx
import matplotlib.pyplot as plt
from abc import ABCMeta, abstractmethod
from scipy.sparse import csc_matrix
from neo4j.v1 import GraphDatabase, basic_auth


driver = GraphDatabase.driver("bolt://localhost", auth = basic_auth("neo4j", "mliu60"))
session = driver.session()


class recommend(object):
	__metaclass__ = ABCMeta

	def __init__(self, session):
		self.personalization = {}
		self.startID = -1
		self.G = nx.Graph()
		self.session = session

	@abstractmethod
	def setStart(self, input):
		pass

	@abstractmethod
	def getProperty(self, ID, srcID, rank):
		pass

	def recommend(self, input, step):
		self.setStart(input)
		assert self.startID > -1
		assert not self.personalization
		self.personalization[self.startID] = 1.0
		listID = [self.startID]

		for i in range(step):
			nextID = []
			for src in listID:
				for dest in list(session.run("match (src)-[]-(dest) where ID(src) = %d return distinct(ID(dest)) as ID" % src)):
					dest = dest['ID']
					if dest not in self.personalization:
						self.G.add_edge(src, dest)
						self.personalization[dest] = 0.0
						nextID.append(dest)
			listID = nextID

		# self.visualize()
		self.rank = nx.pagerank_scipy(self.G, alpha = 0.9, personalization = self.personalization, tol = 1e-4, max_iter = 256)
		# self.showRank()
		return self.filterResult()

	def filterResult(self):
		recommendationList = []
		for k, v in self.rank.iteritems():
			shouldRecommend, prop, pgr = self.getProperty(k, self.startID, v)
			if shouldRecommend:
				recommendationList.append((prop, pgr))

		'''
		for item in recommendationList:
			print item
		'''
		return recommendationList

	def showRank(self):
		for k, v in self.rank.iteritems():
			print k, v

	def visualize(self):
		plt.show(nx.draw(self.G, nodecolor = 'r',edge_color = 'b'))
		


class recommendPaperToResearcher(recommend):
	def setStart(self, input):
		self.startID = session.run("match (r:Researcher {name:'%s'}) return ID(r) as ID" % input).single()["ID"]

	def getProperty(self, ID, srcID, rank):
		if len(list(self.session.run("match (p:Paper)-[a:AuthorOf]-(r:Researcher) where ID(p) = %d and ID(r) = %d return a" % (ID, srcID)))) != 0:
			return False, None, None
		result = list(self.session.run("match (p:Paper) where ID(p) = %d return p.title as title, p.pagerank as PR" % ID))
		if len(result) == 0 or rank < 1e-4 / len(self.G):
			return False, None, None
		assert len(result) == 1
		return True, result[0]["title"], result[0]["PR"]
		

class recommendResearcherToPaper(recommend):
	def setStart(self, input):
		self.startID = session.run("match (p:Paper {title:'%s'}) return ID(p) as ID" % input).single()["ID"]

	def getProperty(self, ID, srcID, rank):
		if len(list(self.session.run("match (p:Paper)-[a:AuthorOf]-(r:Researcher) where ID(r) = %d and ID(p) = %d return a" % (ID, srcID)))) != 0:
			return False, None, None
		result = list(self.session.run("match (r:Researcher) where ID(r) = %d return r.name as name, r.pagerank as PR" % ID))
		if len(result) == 0 or rank < 1e-4 / len(self.G):
			return False, None, None
		assert len(result) == 1
		return True, result[0]["name"], result[0]["PR"]


class recommendResearcherToResearcher(recommend):
	def setStart(self, input):
		self.startID = session.run("match (r:Researcher {name: '%s'} return ID(r) as ID)" % input).single()["ID"]

	def getProperty(self, ID, srcID, rank):
		if ID == srcID:
			return False, None, None
		result = list(self.session.run("match (r:Researcher) where ID(r) = %d return r.name as name, r.pagerank as PR" % ID))
		if len(result) == 0 or rank < 1e-4 / len(self.G):
			return False, None, None
		assert len(result) == 1
		return True, result[0]["name"], result[0]["PR"]


class recommendPaperToPaper(recommend):
	def setStart(self, input):
		self.startID = session.run("match (p:Paper {title: '%s'}) return ID(p) as ID").single()["ID"]
	
	def getProperty(self, ID, srcID, rank):
		if ID == srcID:
			return False, None, None
		result = list(self.session.run("match (p:Paper) where ID(p) = %d return p.title as title, p.pagerank as PR" % ID))
		if len(result) == 0 or rank < 1e-4 / len(self.G):
			return False, None, None
		assert len(result) == 1
		return True, result[0]["title"], result[0]["PR"]


recommender = recommendPaperToResearcher(session)
result = recommender.recommend("Richard Socher", 3)

recommender = recommendResearcherToPaper(session)
result = recommender.recommend("Dynamic Memory Networks for Visual and Textual Question Answering.", 3)

recommender = recommendResearcherToResearcher(session)
result = recommender.recommend("Richard Socher", 4)

recommender = recommendPaperToPaper(session)
result = recommender.recommend("Dynamic Memory Networks for Visual and Textual Question Answering.", 2)


session.close()

