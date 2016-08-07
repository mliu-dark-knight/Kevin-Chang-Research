import networkx as nx
from abc import ABCMeta, abstractmethod


class recommend(object):
	__metaclass__ = ABCMeta

	def __init__(self, session):
		self.personalization = {}
		self.startID = -1
		self.G = nx.Graph()
		self.candidates = {}
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

	@abstractmethod
	def shouldRecommend(self, candidateID):
		pass

	def getProperty(self, candidateID):
		return self.candidates[candidateID]

	def initilizePersonalization(self):
		for node in self.G.nodes():
			self.personalization[node] = 0.0
		self.personalization[self.startID] = 1.0

	def recommend(self, input):
		self.setStart(input)
		assert self.startID > -1
		self.constructGraph()
		assert not self.candidates
		self.generateCandidates()
		# self.visualize()
		assert not self.personalization
		self.initilizePersonalization()
		self.rank = nx.pagerank_scipy(self.G, alpha = 0.9, personalization = self.personalization, tol = 1e-6, max_iter = 256)
		# self.showRank()
		return self.filterResult()

	def filterResult(self):
		recommendationList = []
		for candidateID in self.candidates:
			if self.shouldRecommend(candidateID):
				recommendationList.append(self.getProperty(candidateID))
		return recommendationList

	def showRank(self):
		for k, v in self.rank.iteritems():
			print k, v

	def visualize(self):
		plt.show(nx.draw(self.G, nodecolor = 'r',edge_color = 'b'))
		


class pprPaperToResearcher(recommend):
	def setStart(self, input):
		self.startID = getResearcherByName(input, self.session)

	def constructGraph(self):
		paths = list(self.session.run("match (n0:Researcher)--(n1)--(n2)--(n3) where ID(n0) = %d return ID(n0) as ID0, ID(n1) as ID1, ID(n2) as ID2, ID(n3) as ID3" % self.startID))
		for path in paths:
			self.G.add_edges_from([(path["ID0"], path["ID1"]), (path["ID1"], path["ID2"]), (path["ID2"], path["ID3"])])

	def generateCandidates(self):
		papers = list(self.session.run("match (r:Researcher)-[*1..3]-(p:Paper) where ID(r) = %d and not (r)-[:AuthorOf]-(p) return ID(p) as ID, p.title as title, p.year as year, p.pagerank as PR" % self.startID))
		for paper in papers:
			self.candidates[paper["ID"]] = (paper["title"], paper["year"], paper["PR"])

	def shouldRecommend(self, candidateID):
		rank = self.rank[candidateID]
		return rank >= 0.25 / len(self.G)
		

class pprResearcherToPaper(recommend):
	def setStart(self, input):
		self.startID = getPaperByTitle(input, self.session)

	def constructGraph(self):
		paths = list(self.session.run("match (n0:Paper)--(n1)--(n2)--(n3) where ID(n0) = %d return ID(n0) as ID0, ID(n1) as ID1, ID(n2) as ID2, ID(n3) as ID3" % self.startID))
		for path in paths:
			self.G.add_edges_from([(path["ID0"], path["ID1"]), (path["ID1"], path["ID2"]), (path["ID2"], path["ID3"])])

	def generateCandidates(self):
		researchers = list(self.session.run("match (p:Paper)-[*1..3]-(r:Researcher) where ID(p) = %d and not (r)-[:AuthorOf]-(p) return ID(r) as ID, r.name as name, r.pagerank as PR" % self.startID))
		for researcher in researchers:
			self.candidates[researcher["ID"]] = (researcher["name"], researcher["PR"])

	def shouldRecommend(self, candidateID):
		rank = self.rank[candidateID]
		return rank >= 0.5 / len(self.G)


class pprResearcherToResearcher(recommend):
	def setStart(self, input):
		self.startID = getResearcherByName(input, self.session)

	def constructGraph(self):
		paths = list(self.session.run("match (n0:Researcher)--(n1)--(n2)--(n3)--(n4) where ID(n0) = %d return ID(n0) as ID0, ID(n1) as ID1, ID(n2) as ID2, ID(n3) as ID3, ID(n4) as ID4" % self.startID))
		for path in paths:
			self.G.add_edges_from([(path["ID0"], path["ID1"]), (path["ID1"], path["ID2"]), (path["ID2"], path["ID3"]), (path["ID3"], path["ID4"])])

	def generateCandidates(self):
		researchers = list(self.session.run("match (r1:Researcher)-[*1..4]-(r2:Researcher) where ID(r1) = %d and not ID(r1) = ID(r2) return ID(r2) as ID, r2.name as name, r2.pagerank as PR" % self.startID))
		for researcher in researchers:
			self.candidates[researcher["ID"]] = (researcher["name"], researcher["PR"])

	def shouldRecommend(self, candidateID):
		rank = self.rank[candidateID]
		return rank >= 0.5 / len(self.G)


class pprPaperToPaper(recommend):
	def setStart(self, input):
		self.startID = getPaperByTitle(input, self.session)

	def constructGraph(self):
		paths = list(self.session.run("match (n0:Paper)--(n1)--(n2) where ID(n0) = %d return ID(n0) as ID0, ID(n1) as ID1, ID(n2) as ID2" % self.startID))
		for path in paths:
			self.G.add_edges_from([(path["ID0"], path["ID1"]), (path["ID1"], path["ID2"])])

	def generateCandidates(self):
		papers = list(self.session.run("match (p1:Paper)-[*1..2]-(p2:Paper) where ID(p1) = %d and not ID(p1) = ID(p2) return ID(p2) as ID, p2.title as title, p2.year as year, p2.pagerank as PR" % self.startID))
		for paper in papers:
			self.candidates[paper["ID"]] = (paper["title"], paper["year"], paper["PR"])

	def shouldRecommend(self, candidateID):
		rank = self.rank[candidateID]
		return rank >= 0.25 / len(self.G)



def getResearcherByName(name, session):
	return session.run("match (r:Researcher {name:'%s'}) return ID(r) as ID" % name).single()["ID"]


def getPaperByTitle(title, session):
	return session.run("match (p:Paper {title: '%s'}) return ID(p) as ID" % title).single()["ID"]


