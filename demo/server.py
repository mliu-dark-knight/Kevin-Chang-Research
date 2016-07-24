import demjson
from flask import Flask, request
from flask_restful import reqparse, abort, Api, Resource
from neo4j.v1 import GraphDatabase, basic_auth


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
		assert len(result) <= 1
		return demjson.encode({nodeType: result[0][nodeType], 'pagerank': result[0]['PR']})


# Actually setup the Api resource routing here
driver = GraphDatabase.driver("bolt://localhost", auth = basic_auth("neo4j", "mliu60"))
session = driver.session()
api.add_resource(BasicInfo, '/BasicInfo')


if __name__ == '__main__':
	app.run(debug = False)

session.close()
