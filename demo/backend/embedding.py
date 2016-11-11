import numpy as np
from abc import ABCMeta, abstractmethod
from scipy.spatial.distance import cityblock, euclidean, cosine


class Recommender(object):
	__metaclass__ = ABCMeta

	def __init__(self, session):
		self.startID = -1
		self.startVec = None
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

	def getResearcherByName(self, name):
		vec = self.getCandidateVec()
		pair = self.session.run("match (r:Researcher {name:'%s'}) return ID(r) as ID, r.%s as %s" % (name, vec, vec)).single()
		return pair["ID"], pair[vec]

	def getPaperByTitle(self, title):
		vec = self.getCandidateVec()
		pair = self.session.run("match (p:Paper {title: '%s'}) return ID(p) as ID, p.%s as %s" % (title, vec, vec)).single()
		return pair["ID"], pair[vec]

	def recommend(self, input, limit, rank_policy):
		if rank_policy not in self.func:
			raise ValueError("Ranking policy does not exist.")
		self.startID, self.startVec = self.getStart(input)
		assert self.startID > -1
		candidates = self.generateCandidates()
		candidateList = [self.getFormat(candidate, self.getRank(candidate, rank_policy)) for candidate in candidates]
		if rank_policy == "Inner Product":
			candidateList.sort(key = lambda c: c["score"], reverse = True)
		else:
			candidateList.sort(key = lambda c: c["score"], reverse = False)
		return candidateList[:limit]

	def getRank(self, candidate, rank_policy):
		vec1, vec2 = map(float, self.startVec.split(' ')), map(float, candidate[self.getCandidateVec()].split(' '))
		return self.func[rank_policy](vec1, vec2)



class PaperToResearcher(Recommender):
	def getStart(self, input):
		return self.getResearcherByName(input)

	def generateCandidates(self):
		vec = self.getCandidateVec()
		return list(self.session.run("match (r:Researcher)-[*1..3]-(p:Paper) where ID(r) = %d and exists(p.%s) and not (r)-[:AuthorOf]-(p) return distinct(ID(p)) as ID, p.title as title, p.year as year, p.pagerank as pagerank, p.%s as %s" % (self.startID, vec, vec, vec)))

	def getProperty(self, candidate):
		return (candidate["title"], candidate["year"], candidate["pagerank"])

	def getFormat(self, candidate, score):
		return paperFormat((candidate["title"], candidate["year"], candidate["pagerank"], score))


class ResearcherToPaper(Recommender):
	def getStart(self, input):
		return self.getPaperByTitle(input)

	def generateCandidates(self):
		vec = self.getCandidateVec()
		return list(self.session.run("match (p:Paper)-[*1..3]-(r:Researcher) where ID(p) = %d and exists(r.%s) and not (r)-[:AuthorOf]-(p) return distinct(ID(r)) as ID, r.name as name, r.pagerank as pagerank, r.%s as %s" % (self.startID, vec, vec, vec)))

	def getProperty(self, candidate):
		return (candidate["name"], candidate["pagerank"])

	def getFormat(self, candidate, score):
		return researcherFormat((candidate["name"], candidate["pagerank"], score))


class ResearcherToResearcher(Recommender):
	def __init__(self, session, G):
		Recommender.__init__(self, session)
		self.G = G

	def getStart(self, input):
		return self.getResearcherByName(input)

	def generateCandidates(self):
		vec = self.getCandidateVec()
		return list(self.session.run("match (r1:Researcher)-[*1..4]-(r2:Researcher) where ID(r1) = %d and exists(r2.%s) and not ID(r1) = ID(r2) return distinct(ID(r2)) as ID, r2.name as name, r2.pagerank as pageranks" % (self.startID, vec)))

	def getProperty(self, candidate):
		return (candidate["name"], candidate["pagerank"])

	def getFormat(self, candidate, score):
		return researcherFormat((candidate["name"], candidate["pagerank"], score))

	def generatePaperCandidates(self):
		vec = self.getCandidateVec()
		return list(self.session.run("match (r:Researcher)-[*1..3]-(p:Paper) where ID(r) = %d and exists(p.%s) and not (r)-[:AuthorOf]-(p) return distinct(ID(p)) as ID, p.%s as %s" % (self.startID, vec, vec, vec)))

	def recommend(self, input, limit, rank_policy):
		if rank_policy not in self.func:
			raise ValueError("Ranking policy does not exist.")
		self.startID, self.startVec = self.getStart(input)
		assert self.startID > -1
		candidates = self.generateCandidates()

		paperCandidates = self.generatePaperCandidates()
		paperList = [{"ID": candidate["ID"], "score": self.getRank(candidate, rank_policy)} for candidate in paperCandidates]
		if rank_policy == "Inner Product":
			paperList.sort(key = lambda c: c["score"], reverse = True)
		else:
			paperList.sort(key = lambda c: c["score"], reverse = False)
		paperSeeds = [paper["ID"] for paper in paperList[:20]]

		rank = self.G.personalized_pagerank(vertices = np.array([candidate["ID"] for candidate in candidates]), directed = False, damping = 0.9, reset_vertices = [self.startID] + paperSeeds)
		candidateList = [self.getFormat(candidate[i], rank[i]) for i in xrange(len(candidates))]
		candidateList.sort(key = lambda c: c["score"], reverse = True)

		return candidateList[:limit]


class PaperToPaper(Recommender):
	def getStart(self, input):
		return self.getPaperByTitle(input)

	def generateCandidates(self):
		vec = self.getCandidateVec()
		return list(self.session.run("match (p1:Paper)-[*1..2]-(p2:Paper) where ID(p1) = %d and exists(p2.%s) and not ID(p1) = ID(p2) return distinct(ID(p2)) as ID, p2.title as title, p2.year as year, p2.pagerank as pagerank, p2.%s as %s" % (self.startID, vec, vec, vec)))

	def getProperty(self, candidate):
		return (candidate["title"], candidate["year"], candidate["pagerank"])

	def getFormat(self, candidate, score):
		return paperFormat((candidate["title"], candidate["year"], candidate["pagerank"], score))



class node2vecRecommender(Recommender):
	def getCandidateVec(self):
		return "node2vec"

class doc2vecRecommender(Recommender):
	def getCandidateVec(self):
		return "doc2vec"

class fastppvRecommender(Recommender):
	def getCandidateVec(self):
		return "fastppv"

class LDARecommender(Recommender):
	def getCandidateVec(self):
		return "LDA"

class jointRecommender(Recommender):
	def getCandidateVec(self):
		return "joint"



class node2vecPaperToResearcher(PaperToResearcher, node2vecRecommender):
	pass

class node2vecResearcherToPaper(ResearcherToPaper, node2vecRecommender):
	pass

class node2vecResearcherToResearcher(ResearcherToResearcher, node2vecRecommender):
	pass

class node2vecPaperToPaper(PaperToPaper, node2vecRecommender):
	pass

class doc2vecPaperToResearcher(PaperToResearcher, doc2vecRecommender):
	pass

class doc2vecResearcherToPaper(ResearcherToPaper, doc2vecRecommender):
	pass

class doc2vecResearcherToResearcher(ResearcherToResearcher, doc2vecRecommender):
	pass

class doc2vecPaperToPaper(PaperToPaper, doc2vecRecommender):
	pass

class fastppvPaperToResearcher(PaperToResearcher, fastppvRecommender):
	pass

class fastppvResearcherToPaper(ResearcherToPaper, fastppvRecommender):
	pass

class fastppvResearcherToResearcher(ResearcherToResearcher, fastppvRecommender):
	pass

class fastppvPaperToPaper(PaperToPaper, fastppvRecommender):
	pass

class LDAPaperToResearcher(PaperToResearcher, LDARecommender):
	pass

class LDAResearcherToPaper(ResearcherToPaper, LDARecommender):
	pass

class LDAResearcherToResearcher(ResearcherToResearcher, LDARecommender):
	pass

class LDAPaperToPaper(PaperToPaper, LDARecommender):
	pass

class jointPaperToResearcher(PaperToResearcher, jointRecommender):
	pass

class jointResearcherToPaper(ResearcherToPaper, jointRecommender):
	pass

class jointResearcherToResearcher(ResearcherToResearcher, jointRecommender):
	pass

class jointPaperToPaper(PaperToPaper, jointRecommender):
	pass




def researcherFormat(result):
	(n, r, s) = result
	return {"name": n, "pagerank": r, "score": s}


def paperFormat(result):
	(t, y, r, s) = result
	return {"title": t, "year": y, "pagerank": r, "score": s}




