import json
import copy
import numpy as np
from igraph import *
from flask import Flask, request
from flask_restful import reqparse, abort, Api, Resource
from neo4j.v1 import GraphDatabase, basic_auth
from scipy.spatial.distance import cityblock, euclidean, cosine
from abc import ABCMeta, abstractmethod
from personalized_pagerank import fullpprPaperToResearcher, fullpprResearcherToResearcher, fullpprResearcherToPaper, fullpprPaperToPaper
from embedding import *

app = Flask(__name__)
api = Api(app)


parser = reqparse.RequestParser()
nodes = {'Researcher': 'name', 'Paper': 'title', 'Conference': 'conference'}


class BasicInfo(Resource):
	def __init__(self):
		super(Resource, self).__init__()
		self.parser = copy.deepcopy(parser)
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


class PublicationHistory(Resource):
	def __init__(self):
		super(Resource, self).__init__()
		self.parser = copy.deepcopy(parser)
		for arg in ['node', 'name', 'conference', 'limit']:
			self.parser.add_argument(arg)

	def get(self):
		args = self.parser.parse_args()
		node = args['node']
		nodeType = nodes[node]
		nodeKey = args[nodeType]
		limit = int(args['limit'])
		results = list(session.run(
			"match (n:%s)-[r]-(p:Paper) where n.%s = '%s' return p.title as title, p.year as year, p.pagerank as pagerank, r.weight as weight order by r.weight desc limit %d"
			 % (node, nodeType, nodeKey, limit)))
		try:
			assert len(results) > 0
		except:
			raise ValueError("%s does not exist or does not have associated papers" % node)
		return json.dumps([{'title': result['title'], 'year': result['year'], 'pagerank': result['pagerank'], 'score': result['weight']} for result in results])


class CompareEmbedding(Resource):
	def __init__(self):
		super(Resource, self).__init__()
		self.parser = copy.deepcopy(parser)
		for arg in ['node1', 'node2', 'name1', 'name2', 'title1', 'title2', 'conference1', 'conference2']:
			self.parser.add_argument(arg)

	def getVector(self, node, nodeType, nodeKey):
		vec = self.getVecName()
		return list(session.run("match (n:%s) where n.%s = '%s' return n.%s as %s" % (node, nodeType, nodeKey, vec, vec)))

	@abstractmethod
	def getVecName(self):
		pass

	def get(self):
		args = self.parser.parse_args()
		node1, node2 = args['node1'], args['node2']
		nodeType1, nodeType2 = nodes[node1], nodes[node2]
		nodeKey1, nodeKey2 = args[nodeType1 + '1'], args[nodeType2 + '2']
		result1 = self.getVector(node1, nodeType1, nodeKey1)
		try:
			assert len(result1) > 0
		except:
			raise ValueError("%s does not exist in database" % node1)
		result2 = self.getVector(node2, nodeType2, nodeKey2)
		try:
			assert len(result2) > 0
		except:
			raise ValueError("%s does not exist in database" % node2)
		result1 = np.array(map(float, result1[0][self.getVecName()].split(' ')))
		result2 = np.array(map(float, result2[0][self.getVecName()].split(' ')))
		diff = ','.join(map(str, result1 - result2))
		inner = np.inner(result1, result2)
		l1, l2, cos = cityblock(result1, result2), euclidean(result1, result2), cosine(result1, result2)
		return json.dumps({'Difference': diff, 'Manhattan Distance': l1, 'Euclidean Distance': l2, 'Cosine Distance': cos, 'Inner Product': inner})

class CompareNode2vec(CompareEmbedding):
	def getVecName(self):
		return "node2vec"
		
class CompareDoc2vec(CompareEmbedding):
	def getVecName(self):
		return "doc2vec"

class CompareCollaborativeFiltering(CompareEmbedding):
	def getVecName(self):
		return "fastppv"

class CompareLDA(CompareEmbedding):
	def getVecName(self):
		return "LDA"



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
		self.parser = copy.deepcopy(parser)
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
		self.parser = copy.deepcopy(parser)
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

class doc2vecRecommendPtoR(RecommendPtoR, embeddingBasedRecommender):
	def getRecommender(self):
		return doc2vecPaperToResearcher(session)

class fastppvRecommendPtoR(RecommendPtoR, embeddingBasedRecommender):
	def getRecommender(self):
		return fastppvPaperToResearcher(session)

class LDARecommendPtoR(RecommendPtoR, embeddingBasedRecommender):
	def getRecommender(self):
		return LDAPaperToResearcher(session)



class RecommendRtoR(Recommender):
	def getKey(self, args):
		return args['name']

class fullpprRecommendRtoR(RecommendRtoR, rankBasedRecommender):
	def getRecommender(self):
		return fullpprResearcherToResearcher(session, G)

class node2vecRecommendRtoR(RecommendRtoR, embeddingBasedRecommender):
	def getRecommender(self):
		return node2vecResearcherToResearcher(session)

class doc2vecRecommendRtoR(RecommendRtoR, embeddingBasedRecommender):
	def getRecommender(self):
		return doc2vecResearcherToResearcher(session)

class fastppvRecommendRtoR(RecommendRtoR, embeddingBasedRecommender):
	def getRecommender(self):
		return fastppvResearcherToResearcher(session)

class LDARecommendRtoR(RecommendRtoR, embeddingBasedRecommender):
	def getRecommender(self):
		return LDAResearcherToResearcher(session)



class RecommendRtoP(Recommender):
	def getKey(self, args):
		return args['title']

class fullpprRecommendRtoP(RecommendRtoP, rankBasedRecommender):
	def getRecommender(self):
		return fullpprResearcherToPaper(session, G)

class node2vecRecommendRtoP(RecommendRtoP, embeddingBasedRecommender):
	def getRecommender(self):
		return node2vecResearcherToPaper(session)

class doc2vecRecommendRtoP(RecommendRtoP, embeddingBasedRecommender):
	def getRecommender(self):
		return doc2vecResearcherToPaper(session)

class fastppvRecommendRtoP(RecommendRtoP, embeddingBasedRecommender):
	def getRecommender(self):
		return fastppvResearcherToPaper(session)

class LDARecommendRtoP(RecommendRtoP, embeddingBasedRecommender):
	def getRecommender(self):
		return LDAResearcherToPaper(session)



class RecommendPtoP(Recommender):
	def getKey(self, args):
		return args['title']

class fullpprRecommendPtoP(RecommendPtoP, rankBasedRecommender):
	def getRecommender(self):
		return fullpprPaperToPaper(session, G)

class node2vecRecommendPtoP(RecommendPtoP, embeddingBasedRecommender):
	def getRecommender(self):
		return node2vecPaperToPaper(session)

class doc2vecRecommendPtoP(RecommendPtoP, embeddingBasedRecommender):
	def getRecommender(self):
		return doc2vecPaperToPaper

class fastppvRecommendPtoP(RecommendPtoP, embeddingBasedRecommender):
	def getRecommender(self):
		return fastppvPaperToPaper(session)

class LDARecommendPtoP(RecommendPtoP, embeddingBasedRecommender):
	def getRecommender(self):
		return LDAPaperToPaper(session)




# Actually setup the Api resource routing here
driver = GraphDatabase.driver("bolt://localhost", auth = basic_auth("neo4j", "mliu60"))
session = driver.session()

# load graph and initialized personalization
G = Graph.Read_Ncol('../../karate.edgelist', weights=True, directed = False)

allApi = {'/BasicInfo': BasicInfo, 
		  '/PublicationHistory': PublicationHistory, 
		  '/CompareEmbedding/node2vec': CompareNode2vec,
		  '/CompareEmbedding/doc2vec': CompareDoc2vec,
		  '/CompareEmbedding/fastppv': CompareCollaborativeFiltering,
		  '/CompareEmbedding/LDA': CompareLDA,
		  '/fullpprRecommend/PtoR': fullpprRecommendPtoR,
		  '/fullpprRecommend/RtoR': fullpprRecommendRtoR,
		  '/fullpprRecommend/RtoP': fullpprRecommendRtoP,
		  '/fullpprRecommend/PtoP': fullpprRecommendPtoP,
		  '/node2vecRecommend/PtoR': node2vecRecommendPtoR,
		  '/node2vecRecommend/RtoR': node2vecRecommendRtoR,
		  '/node2vecRecommend/RtoP': node2vecRecommendRtoP,
		  '/node2vecRecommend/PtoP': node2vecRecommendPtoP,
		  '/doc2vecRecommend/PtoR': doc2vecRecommendPtoR,
		  '/doc2vecRecommend/RtoR': doc2vecRecommendRtoR,
		  '/doc2vecRecommend/RtoP': doc2vecRecommendRtoP,
		  '/doc2vecRecommend/PtoP': doc2vecRecommendPtoP,
		  '/fastppvRecommend/PtoR': fastppvRecommendPtoR,
		  '/fastppvRecommend/RtoR': fastppvRecommendRtoR,
		  '/fastppvRecommend/RtoP': fastppvRecommendRtoP,
		  '/fastppvRecommend/PtoP': fastppvRecommendPtoP,
		  '/LDARecommend/PtoR': LDARecommendPtoR,
		  '/LDARecommend/RtoR': LDARecommendRtoR,
		  '/LDARecommend/RtoP': LDARecommendRtoP,
		  '/LDARecommend/PtoP': LDARecommendPtoP
		 }
for k, v in allApi.iteritems():
	api.add_resource(v, k)



if __name__ == '__main__':
	app.run(debug = False)

session.close()

