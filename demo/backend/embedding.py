from abc import ABCMeta, abstractmethod
from scipy.spatial.distance import cosine


class recommend(object):
	__metaclass__ = ABCMeta

	def __init__(self, session):
		self.startID = -1
		self.startVec = None
		self.candidatesInfo = {}
		self.candidatesVec = {}
		self.session = session

	@abstractmethod
	def getStart(self, input):
		pass

	@abstractmethod
	def generateCandidates(self):
		pass

	@abstractmethod
	def shouldRecommend(self, startVec, candidateVec):
		pass

	@abstractmethod
	def getProperty(self, candidate):
		pass

	def recommend(self, input):
		self.startID, self.startVec = self.getStart(input)
		assert self.startID > -1
		recommendationList = []
		for candidate in self.generateCandidates():
			if self.shouldRecommend(candidate):
				recommendationList.append(self.getProperty(candidate))
		return recommendationList



class node2vecPaperToResearcher(recommend):
	def getStart(self, input):
		return getResearcherByName(input, self.session)

	def generateCandidates(self):
		return list(self.session.run("match (r:Researcher)-[*1..3]-(p:Paper) where ID(r) = %d and not (r)-[:AuthorOf]-(p) return ID(p) as ID, p.title as title, p.year as year, p.pagerank as PR, p.node2vec as vec" % self.startID))

	def shouldRecommend(self, candidate):
		return stringCos(candidate["vec"], self.startVec) < 16e-2

	def getProperty(self, candidate):
		return (candidate["title"], candidate["year"], candidate["PR"])



class node2vecResearcherToPaper(recommend):
	def getStart(self, input):
		return getPaperByTitle(input, self.session)

	def generateCandidates(self):
		return list(self.session.run("match (p:Paper)-[*1..3]-(r:Researcher) where ID(p) = %d and not (r)-[:AuthorOf]-(p) return ID(r) as ID, r.name as name, r.pagerank as PR, r.node2vec as vec" % self.startID))

	def shouldRecommend(self, candidate):
		return stringCos(candidate["vec"], self.startVec) < 16e-2

	def getProperty(self, candidate):
		return (candidate["name"], candidate["PR"])



class node2vecResearcherToResearcher(recommend):
	def getStart(self, input):
		return getResearcherByName(input, self.session)

	def generateCandidates(self):
		return list(self.session.run("match (r1:Researcher)-[*1..4]-(r2:Researcher) where ID(r1) = %d and not ID(r1) = ID(r2) return ID(r2) as ID, r2.name as name, r2.pagerank as PR, r2.node2vec as vec" % self.startID))

	def shouldRecommend(self, candidate):
		return stringCos(candidate["vec"], self.startVec) < 16e-2

	def getProperty(self, candidate):
		return (candidate["name"], candidate["PR"])



class node2vecPaperToPaper(recommend):
	def getStart(self, input):
		return getPaperByTitle(input, self.session)

	def generateCandidates(self):
		return list(self.session.run("match (p1:Paper)-[*1..2]-(p2:Paper) where ID(p1) = %d and not ID(p1) = ID(p2) return ID(p2) as ID, p2.title as title, p2.year as year, p2.pagerank as PR, p2.node2vec as vec" % self.startID))

	def shouldRecommend(self, candidate):
		return stringCos(candidate["vec"], self.startVec) < 16e-2

	def getProperty(self, candidate):
		return (candidate["title"], candidate["year"], candidate["PR"])




def getResearcherByName(name, session):
	pair = session.run("match (r:Researcher {name:'%s'}) return ID(r) as ID, r.node2vec as vec" % name).single()
	return pair["ID"], pair["vec"]


def getPaperByTitle(title, session):
	pair = session.run("match (p:Paper {title: '%s'}) return ID(p) as ID, p.node2vec as vec" % title).single()
	return pair["ID"], pair["vec"]


def stringCos(vec1, vec2):
	vec1, vec2 = map(float, vec1.split(' ')), map(float, vec2.split(' '))
	return cosine(vec1, vec2)



