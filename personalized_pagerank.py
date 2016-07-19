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
	def getProperty(self, ID, rank):
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
		self.rank = nx.pagerank_scipy(self.G, alpha = 0.9, personalization = self.personalization, tol = 10**(-2), max_iter = 32)
		# self.showRank()
		return self.filterResult()

	def filterResult(self):
		recommendationList = []
		for k, v in self.rank.iteritems():
			shouldRecommend, prop = self.getProperty(k, v)
			if shouldRecommend:
				recommendationList.append(prop)

		for item in recommendationList:
			print item
		return recommendationList

	def showRank(self):
		for k, v in self.rank.iteritems():
			print k, v

	def visualize(self):
		plt.show(nx.draw(self.G, nodecolor = 'r',edge_color = 'b'))
		


class recommendFromResearcher(recommend):
	def setStart(self, input):
		self.startID = session.run("match (r:Researcher {name:'%s'}) return ID(r) as ID" % input).single()["ID"]

	def getProperty(self, ID, rank):
		result = list(self.session.run("match (p:Paper) where ID(p) = %d return p.title as title" % ID))
		if len(result) == 0 or rank < 10**(-6):
			return False, []
		assert len(result) == 1
		return True, result[0]["title"]
		

class recommendToResearcher(recommend):
	def setStart(self, input):
		self.startID = session.run("match (p:Paper {title:'%s'}) return ID(p) as ID" % input).single()["ID"]

	def getProperty(self, ID, rank):
		result = list(self.session.run("match (r:Researcher) where ID(r) = %d return r.name as name" % ID))
		if len(result) == 0 or rank < 10**(-5):
			return False, []
		assert len(result) == 1
		return True, result[0]["name"]
		


recommender = recommendFromResearcher(session)
result = recommender.recommend("Alfredo Cano", 3)

recommender = recommendToResearcher(session)
result = recommender.recommend("Level Construction of Decision Trees in a Partition-based Framework for Classi cation.", 3)


session.close()
