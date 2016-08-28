import json
import numpy as np
from igraph import *
from flask import Flask, request
from flask_restful import reqparse, abort, Api, Resource
from neo4j.v1 import GraphDatabase, basic_auth
from scipy.spatial.distance import cityblock, euclidean, cosine
from abc import ABCMeta, abstractmethod
from personalized_pagerank import fullpprPaperToResearcher, fullpprResearcherToResearcher, fullpprResearcherToPaper, fullpprPaperToPaper
from embedding import node2vecPaperToResearcher, node2vecResearcherToResearcher, node2vecResearcherToPaper, node2vecPaperToPaper, \
					  fastppvPaperToResearcher, fastppvResearcherToResearcher, fastppvResearcherToPaper, fastppvPaperToPaper


app = Flask(__name__)
api = Api(app)


parser = reqparse.RequestParser()
nodes = {'Researcher': 'name', 'Paper': 'title', 'Conference': 'conference'}


class BasicInfo(Resource):
	def __init__(self):
		super(Resource, self).__init__()
		self.parser = parser
		for arg in ['node', 'name', 'title', 'conference']:
			self.parser.add_argument(arg)

	def get(self):
		args = self.parser.parse_args()
		node = args['node']
		nodeType = nodes[node]
		nodeKey = args[nodeType]
		result = list(session.run("match (n:%s) where n.%s = '%s' return n.%s as %s, n.pagerank as PR" % (node, nodeType, nodeKey, nodeType, nodeType)))
		try:
			assert len(result) == 1
		except:
			raise ValueError("%s does not exist in database" % node)

		return json.dumps({nodeType: result[0][nodeType], 'pagerank': result[0]['PR']})


class CompareEmbedding(Resource):
	def __init__(self):
		super(Resource, self).__init__()
		self.parser = parser
		for arg in ['node1', 'node2', 'name1', 'name2', 'title1', 'title2', 'conference1', 'conference2']:
			self.parser.add_argument(arg)

	@abstractmethod
	def getVector(self, node, nodeType, nodeKey):
		pass

	def get(self):
		args = self.parser.parse_args()
		node1, node2 = args['node1'], args['node2']
		nodeType1, nodeType2 = nodes[node1], nodes[node2]
		nodeKey1, nodeKey2 = args[nodeType1 + '1'], args[nodeType2 + '2']
		result1 = self.getVector(node1, nodeType1, nodeKey1)
		try:
			assert len(result1) == 1
		except:
			raise ValueError("%s does not exist in database" % node)
		result2 = self.getVector(node2, nodeType2, nodeKey2)
		try:
			assert len(result2) == 1
		except:
			raise ValueError("%s does not exist in database" % node)
		result1 = np.array(map(float, result1[0]['node2vec'].split(' ')))
		result2 = np.array(map(float, result2[0]['node2vec'].split(' ')))
		diff = ','.join(map(str, result1 - result2))
		inner = np.inner(result1, result2)
		l1, l2, cos = cityblock(result1, result2), euclidean(result1, result2), cosine(result1, result2)
		return json.dumps({'Difference': diff, 'Manhattan Distance': l1, 'Euclidean Distance': l2, 'Cosine Distance': cos, 'Inner Product': inner})

class CompareNode2vec(CompareEmbedding):
	def getVector(self, node, nodeType, nodeKey):
		return list(session.run("match (n:%s) where n.%s = '%s' return n.node2vec as node2vec" % (node, nodeType, nodeKey)))

class CompareCollaborativeFiltering(CompareEmbedding):
	def getVector(self, node, nodeType, nodeKey):
		return list(session.run("match (n:%s) where n.%s = '%s' return n.fastppv as fastppv" % (node, nodeType, nodeKey)))



class Recommender(Resource):
	@abstractmethod
	def getKey(self, args):
		pass

	@abstractmethod
	def getRecommender(self):
		pass

	@abstractmethod
	def get(self):
		pass


class rankBasedRecommender(Recommender):
	def __init__(self):
		super(Recommender, self).__init__()
		self.parser = parser
		for arg in ['name', 'title', 'conference', 'limit']:
			self.parser.add_argument(arg)

	def get(self):
		args = self.parser.parse_args()
		limit = int(args['limit'])
		key = self.getKey(args)
		recommender = self.getRecommender()
		return json.dumps(recommender.recommend(key, limit))	


class embeddingBasedRecommender(Recommender):
	def __init__(self):
		super(Recommender, self).__init__()
		self.parser = parser
		for arg in ['name', 'title', 'conference', 'limit', 'rank_criterion']:
			self.parser.add_argument(arg)

	def get(self):
		args = self.parser.parse_args()
		limit = int(args['limit'])
		rank_policy = args['rank_criterion']
		key = self.getKey(args)
		recommender = self.getRecommender()
		return json.dumps(recommender.recommend(key, limit, rank_policy))	




class RecommendPtoR(Recommender):
	def getKey(self, args):
		return args['name']

class fullpprRecommendPtoR(RecommendPtoR, rankBasedRecommender):
	def getRecommender(self):
		return fullpprPaperToResearcher(session, G)

class node2vecRecommendPtoR(RecommendPtoR, embeddingBasedRecommender):
	def getRecommender(self):
		return node2vecPaperToResearcher(session)

class fastppvRecommendPtoR(RecommendPtoR, embeddingBasedRecommender):
	def getRecommender(self):
		return fastppvPaperToResearcher



class RecommendRtoR(Recommender):
	def getKey(self, args):
		return args['name']

class fullpprRecommendRtoR(RecommendRtoR, rankBasedRecommender):
	def getRecommender(self):
		return fullpprResearcherToResearcher(session, G)

class node2vecRecommendRtoR(RecommendRtoR, embeddingBasedRecommender):
	def getRecommender(self):
		return node2vecResearcherToResearcher(session)

class fastppvRecommendRtoR(RecommendRtoR, embeddingBasedRecommender):
	def getRecommender(self):
		return fastppvResearcherToResearcher



class RecommendRtoP(Recommender):
	def getKey(self, args):
		return args['title']

class fullpprRecommendRtoP(RecommendRtoP, rankBasedRecommender):
	def getRecommender(self):
		return fullpprResearcherToPaper(session, G)

class node2vecRecommendRtoP(RecommendRtoP, embeddingBasedRecommender):
	def getRecommender(self):
		return node2vecResearcherToPaper(session)

class fastppvRecommendRtoP(RecommendRtoP, embeddingBasedRecommender):
	def getRecommender(self):
		return fastppvResearcherToPaper



class RecommendPtoP(Recommender):
	def getKey(self, args):
		return args['title']

class fullpprRecommendPtoP(RecommendPtoP, rankBasedRecommender):
	def getRecommender(self):
		return fullpprPaperToPaper(session, G)

class node2vecRecommendPtoP(RecommendPtoP, embeddingBasedRecommender):
	def getRecommender(self):
		return node2vecPaperToPaper(session)

class fastppvRecommendPtoP(RecommendPtoP, embeddingBasedRecommender):
	def getRecommender(self):
		return fastppvPaperToPaper(session)




# Actually setup the Api resource routing here
driver = GraphDatabase.driver("bolt://localhost", auth = basic_auth("neo4j", "mliu60"))
session = driver.session()

# load graph and initialized personalization
G = Graph.Read_Ncol('../../karate.edgelist', directed = False)

allApi = {'/BasicInfo': BasicInfo, 
		  '/CompareEmbedding/node2vec': CompareNode2vec,
		  '/CompareEmbedding/fastppv': CompareCollaborativeFiltering,
		  '/fullpprRecommend/PtoR': fullpprRecommendPtoR,
		  '/fullpprRecommend/RtoR': fullpprRecommendRtoR,
		  '/fullpprRecommend/RtoP': fullpprRecommendRtoP,
		  '/fullpprRecommend/PtoP': fullpprRecommendPtoP,
		  '/node2vecRecommend/PtoR': node2vecRecommendPtoR,
		  '/node2vecRecommend/RtoR': node2vecRecommendRtoR,
		  '/node2vecRecommend/RtoP': node2vecRecommendRtoP,
		  '/node2vecRecommend/PtoP': node2vecRecommendPtoP,
		  '/fastppvRecommend/PtoP': fastppvRecommendPtoP,
		  '/fastppvRecommend/RtoR': fastppvRecommendRtoR,
		  '/fastppvRecommend/RtoP': fastppvRecommendRtoP,
		  '/fastppvRecommend/PtoP': fastppvRecommendPtoP
		 }
for k, v in allApi.iteritems():
	api.add_resource(v, k)



if __name__ == '__main__':
	app.run(debug = False)

session.close()

