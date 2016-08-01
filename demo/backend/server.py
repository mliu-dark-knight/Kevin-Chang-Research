import json
import numpy as np
from flask import Flask, request
from flask_restful import reqparse, abort, Api, Resource
from neo4j.v1 import GraphDatabase, basic_auth
from scipy.spatial.distance import cityblock, euclidean, cosine
from personalized_pagerank import pprPaperToResearcher, pprResearcherToResearcher, pprResearcherToPaper, pprPaperToPaper


app = Flask(__name__)
api = Api(app)


parser = reqparse.RequestParser()
for arg in ['name', 'title', 'conference']:
	parser.add_argument(arg)

nodes = {'Researcher': 'name', 'Paper': 'title', 'Conference': 'conference'}


class BasicInfo(Resource):
	def get(self):
		localparser = parser
		localparser.add_argument('node')
		nodes = {'Researcher': 'name', 'Paper': 'title', 'Conference': 'conference'}
		args = localparser.parse_args()
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
	def get(self):
		localparser = parser
		for arg in ['node', 'name1', 'name2', 'title1', 'title2', 'conference1', 'conference2']:
			localparser.add_argument(arg)
		args = localparser.parse_args()
		node = args['node']
		nodeType = nodes[node]
		nodeKey1, nodeKey2 = args[nodeType + '1'], args[nodeType + '2']
		result1 = list(session.run("match (n:%s) where n.%s = '%s' return n.node2vec as node2vec" % (node, nodeType, nodeKey1)))
		try:
			assert len(result1) == 1
		except:
			raise ValueError("%s does not exist in database" % node)
		result2 = list(session.run("match (n:%s) where n.%s = '%s' return n.node2vec as node2vec" % (node, nodeType, nodeKey2)))
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


class RecommendPtoR(Resource):
	def get(self):
		args = parser.parse_args()
		name = args['name']
		recommender = pprPaperToResearcher(session)
		result = recommender.recommend(name)
		return json.dumps([{'title': t, 'pagerank': r} for (t, r) in result])


class RecommendRtoR(Resource):
	def get(self):
		args = parser.parse_args()
		name = args['name']
		recommender = pprResearcherToResearcher(session)
		result = recommender.recommend(name)
		return json.dumps([{'name': n, 'pagerank': r} for (n, r) in result])


class RecommendRtoP(Resource):
	def get(self):
		args = parser.parse_args()
		title = args['title']
		recommender = pprResearcherToPaper(session)
		result = recommender.recommend(title)
		return json.dumps([{'name': n, 'pagerank': r} for (n, r) in result])


class RecommendPtoP(Resource):
	def get(self):
		args = parser.parse_args()
		title = args['title']
		recommender = pprPaperToPaper(session)
		result = recommender.recommend(title)
		return json.dumps([{'title': t, 'pagerank': r} for (t, r) in result])


# Actually setup the Api resource routing here
driver = GraphDatabase.driver("bolt://localhost", auth = basic_auth("neo4j", "mliu60"))
session = driver.session()

allApi = {'/BasicInfo': BasicInfo, 
		  '/CompareEmbedding': CompareEmbedding, 
		  '/Recommend/PtoR': RecommendPtoR, 
		  '/Recommend/RtoR': RecommendRtoR, 
		  '/Recommend/RtoP': RecommendRtoP, 
		  '/Recommend/PtoP': RecommendPtoP}
for k, v in allApi.iteritems():
	api.add_resource(v, k)



if __name__ == '__main__':
	app.run(debug = False)

session.close()

