import numpy as np
from abc import ABCMeta, abstractmethod
from scipy.spatial.distance import cityblock, euclidean, cosine


class Supporter(object):
	__metaclass__ = ABCMeta

	def __init__(self, session):
		self.startVec = None
		self.session = session

	@abstractmethod
	def getCandidateVec(self):
		pass

	def getFormat(self, supporter, score):
		return {"title": supporter["title"], "year": supporter["year"], "score": score}

	def getStart(self, name):
		vec = self.getCandidateVec()
		return self.session.run("match (r:Researcher {name: '%s'}) return r.%s as %s" % (name, vec, vec)).single()[vec]

	def support(self, query, candidate, limit):
		self.startVec = self.getStart(query)
		vec = self.getCandidateVec()
		supporters = list(self.session.run("match (r1:Researcher {name: '%s'})--(p:Paper)-[*1..3]-(r2:Researcher {name:'%s'}) return distinct(ID(p)) as ID, p.title as title, p.year as year, p.%s as %s" % (candidate, query, vec, vec)))
		supporterList = [self.getFormat(supporter, self.getRank(supporter)) for supporter in supporters]
		supporterList.sort(key=lambda c: c["score"], reverse=False)
		return supporterList[:limit]

	def getRank(self, supporter):
		vec1, vec2 = map(float, self.startVec.split(' ')), map(float, supporter[self.getCandidateVec()].split(' '))
		return cosine(vec1, vec2)


class jointSupporter(Supporter):
	def getCandidateVec(self):
		return "joint"
