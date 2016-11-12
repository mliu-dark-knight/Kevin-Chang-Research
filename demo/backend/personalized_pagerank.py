import numpy as np
from abc import ABCMeta, abstractmethod


class Recommender(object):
	__metaclass__ = ABCMeta

	def __init__(self, session, G):
		self.startID = -1
		self.G = G
		self.session = session

	@abstractmethod
	def getStart(self, input):
		pass

	@abstractmethod
	def generateCandidates(self):
		pass

	@abstractmethod
	def getProperty(self, candidate):
		pass

	@abstractmethod
	def getFormat(self, candidate, score):
		pass

	def recommend(self, input, limit):
		self.startID = self.getStart(input)
		assert self.startID > -1
		candidates = self.generateCandidates()
		rank = self.G.personalized_pagerank(vertices = np.array([candidate["ID"] for candidate in candidates]), weights='weight', directed = False, damping = 0.9, reset_vertices = self.startID, implementation = "power", niter = 64)
		candidateList = [self.getFormat(candidates[i], rank[i]) for i in xrange(len(candidates))]
		candidateList.sort(key = lambda c: c["score"], reverse = True)
		return candidateList[:limit]


class fullpprPaperToResearcher(Recommender):
	def getStart(self, input):
		return getResearcherByName(input, self.session)

	def generateCandidates(self):
		return list(self.session.run("match (r:Researcher)-[*1..3]-(p:Paper) where ID(r) = %d and not (r)-[:AuthorOf]-(p) return ID(p) as ID, p.title as title, p.year as year, p.pagerank as pagerank" % self.startID))

	def getProperty(self, candidate):
		return (candidate["title"], candidate["year"], candidate["pagerank"])

	def getFormat(self, candidate, score):
		return paperFormat((candidate["title"], candidate["year"], candidate["pagerank"], score))


class fullpprResearcherToPaper(Recommender):
	def getStart(self, input):
		return getPaperByTitle(input, self.session)

	def generateCandidates(self):
		return list(self.session.run("match (p:Paper)-[*1..3]-(r:Researcher) where ID(p) = %d and not (r)-[:AuthorOf]-(p) return ID(r) as ID, r.name as name, r.pagerank as pagerank" % self.startID))

	def getProperty(self, candidate):
		return (candidate["name"], candidate["pagerank"])

	def getFormat(self, candidate, score):
		return researcherFormat((candidate["name"], candidate["pagerank"], score))


class fullpprResearcherToResearcher(Recommender):
	def getStart(self, input):
		return getResearcherByName(input, self.session)

	def generateCandidates(self):
		return list(self.session.run("match (r1:Researcher)-[*1..4]-(r2:Researcher) where ID(r1) = %d and not ID(r1) = ID(r2) return ID(r2) as ID, r2.name as name, r2.pagerank as pagerank" % self.startID))

	def getProperty(self, candidate):
		return (candidate["name"], candidate["pagerank"])

	def getFormat(self, candidate, score):
		return researcherFormat((candidate["name"], candidate["pagerank"], score))


class fullpprPaperToPaper(Recommender):
	def getStart(self, input):
		return getPaperByTitle(input, self.session)

	def generateCandidates(self):
		return list(self.session.run("match (p1:Paper)-[*1..2]-(p2:Paper) where ID(p1) = %d and not ID(p1) = ID(p2) return ID(p2) as ID, p2.title as title, p2.year as year, p2.pagerank as pagerank" % self.startID))

	def getProperty(self, candidate):
		return (candidate["title"], candidate["year"], candidate["pagerank"])

	def getFormat(self, candidate, score):
		return paperFormat((candidate["title"], candidate["year"], candidate["pagerank"], score))



def getResearcherByName(name, session):
	return session.run("match (r:Researcher {name:'%s'}) return ID(r) as ID" % name).single()["ID"]


def getPaperByTitle(title, session):
	return session.run("match (p:Paper {title: '%s'}) return ID(p) as ID" % title).single()["ID"]


def researcherFormat(result):
	(n, r, s) = result
	return {"name": n, "pagerank": r, "score": s}


def paperFormat(result):
	(t, y, r, s) = result
	return {"title": t, "year": y, "pagerank": r, "score": s}



