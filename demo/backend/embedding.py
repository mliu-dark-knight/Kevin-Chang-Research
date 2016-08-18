import numpy as np
from abc import ABCMeta, abstractmethod
from scipy.spatial.distance import cosine


class Recommender(object):
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
	def getCandidateVec(self):
		pass

	@abstractmethod
	def getProperty(self, candidate):
		pass

	@abstractmethod
	def getRank(self, candidate):
		pass

	@abstractmethod
	def getFormat(self, candidate):
		pass

	def recommend(self, input, limit):
		self.startID, self.startVec = self.getStart(input)
		assert self.startID > -1
		candidates = self.generateCandidates()
		candidateList = []
		num_candidates = len(candidates)
		ranks = np.empty([num_candidates, 2])
		idx = 0
		for candidate in candidates:
			ranks[idx] = np.array([idx, -self.getRank(candidate)])
			candidateList.append(self.getFormat(candidate))
			idx += 1
		assert idx == num_candidates
		print ranks
		ranks = np.sort(ranks, axis = 1)[:limit]
		recommendationList = []
		for r in ranks:
			recommendationList.append(self.getProperty(candidateList[int(r[0])]))
		return recommendationList



class PaperToResearcher(Recommender):
	def getStart(self, input):
		return getResearcherByName(input, self.session)

	def generateCandidates(self):
		vec = self.getCandidateVec()
		return list(self.session.run("match (r:Researcher)-[*1..3]-(p:Paper) where ID(r) = %d and not (r)-[:AuthorOf]-(p) return ID(p) as ID, p.title as title, p.year as year, p.pagerank as pagerank, p.%s as %s" % (self.startID, vec, vec)))

	def getProperty(self, candidate):
		return (candidate["title"], candidate["year"], candidate["pagerank"])

	def getFormat(self, candidate):
		return paperFormat((candidate["title"], candidate["year"], candidate["pagerank"]))


class ResearcherToPaper(Recommender):
	def getStart(self, input):
		return getPaperByTitle(input, self.session)

	def generateCandidates(self):
		vec = self.getCandidateVec()
		return list(self.session.run("match (p:Paper)-[*1..3]-(r:Researcher) where ID(p) = %d and not (r)-[:AuthorOf]-(p) return ID(r) as ID, r.name as name, r.pagerank as pagerank, r.%s as %s" % (self.startID, vec, vec)))

	def getProperty(self, candidate):
		return (candidate["name"], candidate["pagerank"])

	def getFormat(self, candidate):
		return paperFormat((candidate["name"], candidate["pagerank"]))


class ResearcherToResearcher(Recommender):
	def getStart(self, input):
		return getResearcherByName(input, self.session)

	def generateCandidates(self):
		vec = self.getCandidateVec()
		return list(self.session.run("match (p:Paper)-[*1..4]-(r:Researcher) where ID(p) = %d and not (r)-[:AuthorOf]-(p) return ID(r) as ID, r.name as name, r.pagerank as pagerank, r.%s as %s" % (self.startID, vec, vec)))

	def getProperty(self, candidate):
		return (candidate["name"], candidate["pagerank"])

	def getFormat(self, candidate):
		return paperFormat((candidate["name"], candidate["pagerank"]))


class PaperToPaper(Recommender):
	def getStart(self, input):
		return getPaperByTitle(input, self.session)

	def generateCandidates(self):
		vec = self.getCandidateVec()
		return list(self.session.run("match (r:Researcher)-[*1..2]-(p:Paper) where ID(r) = %d and not (r)-[:AuthorOf]-(p) return ID(p) as ID, p.title as title, p.year as year, p.pagerank as pagerank, p.%s as %s" % (self.startID, vec,vec)))

	def getProperty(self, candidate):
		return (candidate["title"], candidate["year"], candidate["pagerank"])

	def getFormat(self, candidate):
		return paperFormat((candidate["title"], candidate["year"], candidate["pagerank"]))



class node2vecRecommender(Recommender):
	def getCandidateVec(self):
		return "node2vec"

	def getRank(self, candidate):
		return stringCos(candidate[self.getCandidateVec()], self.startVec)


class ppvRecommender(Recommender):
	def getCandidateVec(self):
		return "ppv"

	def getRank(self, candidate):
		return stringDot(candidate[self.getCandidateVec()], self.startVec)



class node2vecPaperToResearcher(PaperToResearcher, node2vecRecommender):
	pass

class node2vecResearcherToPaper(ResearcherToPaper, node2vecRecommender):
	pass

class node2vecResearcherToResearcher(ResearcherToResearcher, node2vecRecommender):
	pass

class node2vecPaperToPaper(PaperToPaper, node2vecRecommender):
	pass

class ppvPaperToResearcher(PaperToResearcher, ppvRecommender):
	pass

class ppvResearcherToPaper(ResearcherToPaper, ppvRecommender):
	pass

class ppvResearcherToResearcher(ResearcherToResearcher, ppvRecommender):
	pass

class ppvPaperToPaper(PaperToPaper, ppvRecommender):
	pass



def getResearcherByName(name, session):
	pair = session.run("match (r:Researcher {name:'%s'}) return ID(r) as ID, r.node2vec as vec" % name).single()
	return pair["ID"], pair["vec"]


def getPaperByTitle(title, session):
	pair = session.run("match (p:Paper {title: '%s'}) return ID(p) as ID, p.node2vec as vec" % title).single()
	return pair["ID"], pair["vec"]


def stringCos(vec1, vec2):
	vec1, vec2 = map(float, vec1.split(' ')), map(float, vec2.split(' '))
	return cosine(vec1, vec2)


def stringDot(vec1, vec2):
	vec1, vec2 = map(float, vec1.split(' ')), map(float, vec2.split(' '))
	return np.dot(vec1, vec2)


def researcherFormat(result):
	(n, r) = result
	return {"name": n, "pagerank": r}


def paperFormat(result):
	(t, y, r) = result
	return {"title": t, "year": y, "pagerank": r}




