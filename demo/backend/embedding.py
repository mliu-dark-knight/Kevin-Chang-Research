import numpy as np
from abc import ABCMeta, abstractmethod
from scipy.spatial.distance import cityblock, euclidean, cosine


class Recommender(object):
	__metaclass__ = ABCMeta

	def __init__(self, session):
		self.startID = -1
		self.startVec = None
		self.candidatesInfo = {}
		self.candidatesVec = {}
		self.session = session
		self.func = {"Manhattan Distance": cityblock, "Euclidean Distance": euclidean, "Cosine Distance": cosine, "Inner Product": np.dot}

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
	def getFormat(self, candidate, score):
		pass

	def recommend(self, input, limit, rank_policy):
		if rank_policy not in self.func:
			raise ValueError("Ranking policy does not exist.")
		self.startID, self.startVec = self.getStart(input)
		assert self.startID > -1
		candidates = self.generateCandidates()
		candidateList = []
		for candidate in candidates:
			candidateList.append(self.getFormat(candidate, self.getRank(candidate, rank_policy)))
		candidateList.sort(key = lambda c: c["score"], reverse = False)
		return candidateList[:limit]

	def getRank(self, candidate, rank_policy):
		vec1, vec2 = map(float, self.startVec.split(' ')), map(float, candidate[self.getCandidateVec()].split(' '))
		return self.func[rank_policy](vec1, vec2)



class PaperToResearcher(Recommender):
	def getStart(self, input):
		return getResearcherByName(input, self.session)

	def generateCandidates(self):
		vec = self.getCandidateVec()
		return list(self.session.run("match (r:Researcher)-[*1..3]-(p:Paper) where ID(r) = %d and not (r)-[:AuthorOf]-(p) return distinct(ID(p)) as ID, p.title as title, p.year as year, p.pagerank as pagerank, p.%s as %s" % (self.startID, vec, vec)))

	def getProperty(self, candidate):
		return (candidate["title"], candidate["year"], candidate["pagerank"])

	def getFormat(self, candidate, score):
		return paperFormat((candidate["title"], candidate["year"], candidate["pagerank"], score))


class ResearcherToPaper(Recommender):
	def getStart(self, input):
		return getPaperByTitle(input, self.session)

	def generateCandidates(self):
		vec = self.getCandidateVec()
		return list(self.session.run("match (p:Paper)-[*1..3]-(r:Researcher) where ID(p) = %d and not (r)-[:AuthorOf]-(p) return distinct(ID(r)) as ID, r.name as name, r.pagerank as pagerank, r.%s as %s" % (self.startID, vec, vec)))

	def getProperty(self, candidate):
		return (candidate["name"], candidate["pagerank"])

	def getFormat(self, candidate, score):
		return paperFormat((candidate["name"], candidate["pagerank"], score))


class ResearcherToResearcher(Recommender):
	def getStart(self, input):
		return getResearcherByName(input, self.session)

	def generateCandidates(self):
		vec = self.getCandidateVec()
		return list(self.session.run("match (p:Paper)-[*1..4]-(r:Researcher) where ID(p) = %d and not (r)-[:AuthorOf]-(p) return distinct(ID(r)) as ID, r.name as name, r.pagerank as pagerank, r.%s as %s" % (self.startID, vec, vec)))

	def getProperty(self, candidate):
		return (candidate["name"], candidate["pagerank"])

	def getFormat(self, candidate, score):
		return paperFormat((candidate["name"], candidate["pagerank"], score))


class PaperToPaper(Recommender):
	def getStart(self, input):
		return getPaperByTitle(input, self.session)

	def generateCandidates(self):
		vec = self.getCandidateVec()
		return list(self.session.run("match (r:Researcher)-[*1..2]-(p:Paper) where ID(r) = %d and not (r)-[:AuthorOf]-(p) return distinct(ID(p)) as ID, p.title as title, p.year as year, p.pagerank as pagerank, p.%s as %s" % (self.startID, vec,vec)))

	def getProperty(self, candidate):
		return (candidate["title"], candidate["year"], candidate["pagerank"])

	def getFormat(self, candidate, score):
		return paperFormat((candidate["title"], candidate["year"], candidate["pagerank"], score))



class node2vecRecommender(Recommender):
	def getCandidateVec(self):
		return "node2vec"		

class ppvRecommender(Recommender):
	def getCandidateVec(self):
		return "ppv"



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


def researcherFormat(result):
	(n, r, s) = result
	return {"name": n, "pagerank": r, "score": s}


def paperFormat(result):
	(t, y, r, s) = result
	return {"title": t, "year": y, "pagerank": r, "score": s}




