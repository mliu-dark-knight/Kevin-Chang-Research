import demjson
import json
from flask import Flask, request
from flask_restful import reqparse, abort, Api, Resource
from neo4j.v1 import GraphDatabase, basic_auth
from personalized_pagerank import recommendPaperToResearcher, recommendResearcherToResearcher, recommendResearcherToPaper, recommendPaperToPaper


app = Flask(__name__)
api = Api(app)


parser = reqparse.RequestParser()
for arg in ['node', 'name', 'title', 'conference']:
	parser.add_argument(arg)

class BasicInfo(Resource):
	def get(self):
		nodes = {'Researcher': 'name', 'Paper': 'title', 'Conference': 'conference'}
		args = parser.parse_args()
		node = args['node']
		nodeType = nodes[node]
		nodeKey = args[nodeType]
		result = list(session.run("match (n:%s) where n.%s = '%s' return n.%s as %s, n.pagerank as PR" % (node, nodeType, nodeKey, nodeType, nodeType)))
		try:
			assert len(result) <= 1
		except:
			raise ValueError("%s does not exist in database" % node)

		return demjson.encode({nodeType: result[0][nodeType], 'pagerank': result[0]['PR']})


class RecommendPtoR(Resource):
	def get(self):
		args = parser.parse_args()
		name = args['name']
		recommender = recommendPaperToResearcher(session)
		result = recommender.recommend(name, 3)
		return json.dumps([{'title': t, 'pagerank': r} for (t, r) in result])


class RecommendRtoR(Resource):
	def get(self):
		args = parser.parse_args()
		name = args['name']
		recommender = recommendResearcherToResearcher(session)
		result = recommender.recommend(name, 4)
		return json.dumps([{'name': n, 'pagerank': r} for (n, r) in result])


class RecommendRtoP(Resource):
	def get(self):
		args = parser.parse_args()
		title = args['title']
		recommender = recommendResearcherToPaper(session)
		result = recommender.recommend(title, 3)
		return json.dumps([{'name': n, 'pagerank': r} for (n, r) in result])


class RecommendPtoP(Resource):
	def get(self):
		args = parser.parse_args()
		title = args['title']
		recommender = recommendPaperToPaper(session)
		result = recommender.recommend(title, 2)
		return json.dumps([{'title': t, 'pagerank': r} for (t, r) in result])


# Actually setup the Api resource routing here
driver = GraphDatabase.driver("bolt://localhost", auth = basic_auth("neo4j", "mliu60"))
session = driver.session()

allApi = {'/BasicInfo': BasicInfo, 
		  '/Recommend/PtoR': RecommendPtoR, 
		  '/Recommend/RtoR': RecommendRtoR, 
		  '/Recommend/RtoP': RecommendRtoP, 
		  '/Recommend/PtoP': RecommendPtoP}
for k, v in allApi.iteritems():
	api.add_resource(v, k)



if __name__ == '__main__':
	app.run(debug = False)

session.close()
