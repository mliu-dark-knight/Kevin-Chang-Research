import numpy as np
import networkx as nx
from neo4j.v1 import GraphDatabase, basic_auth

per_session = 50000


driver = GraphDatabase.driver("bolt://localhost", auth = basic_auth("neo4j", "mliu60"))
session = driver.session()

initilize = """
			match (node) set node.pagerank = 0.0
			"""

iterate = """
		  match (node)
		  with collect(distinct node) as pages
		  unwind pages as dest
		  match (source)-[:AuthorOf]-(dest)
		  with collect(distinct source) as sources, dest as dest
		  unwind sources as src
		  match (src)-[r:AuthorOf]-()
		  with src.pagerank / count(r) as points, dest as dest
		  with sum(points) as p, dest as dest
		  set dest.pagerank = 0.15 * dest.pagerank + 0.85 * p;
		  """



G = nx.Graph()
num_node = session.run("match (n) return count(*) as count").single()['count']

for i in range(num_node / per_session + 1):
	lower = i * per_session
	upper = (i+1) * per_session
	for edge in list(session.run("match (src)-->(dest) where ID(src) >= %d and ID(src) < %d "\
								 "return ID(src) as srcID, ID(dest) as destID" % (lower, upper))):
		G.add_edge(edge['srcID'], edge['destID'])
print "Finish setting up graph"

rank = nx.pagerank_scipy(G, alpha = 0.9, tol = 1e-6, max_iter = 256)
print "Finish ranking"

for k, v in rank.iteritems():
	session.run("match (n) where ID(n) = %d set n.pagerank = %f" % (k, v * 100))
print "Finish updating page rank"

session.close()
