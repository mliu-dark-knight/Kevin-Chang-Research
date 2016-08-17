import gc
from neo4j.v1 import GraphDatabase, basic_auth
import numpy as np
from scipy.sparse import csc_matrix


prob_stay = 0.1
out = 1.0 - prob_stay
epoch = 50000
num_iter = 256


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



num_node = session.run("match (n) return count(*) as count").single()['count']
num_edge = session.run("match ()-[r]-() return count(r) as count").single()['count']
print "%d nodes, %d edges" % (num_node, num_edge)

row = np.empty([num_node + num_edge], dtype = int)
col = np.empty([num_node + num_edge], dtype = int)
data = np.empty([num_node + num_edge], dtype = float)


sparse_idx = 0
for x in range(num_node):
	ys = list(session.run("match (src)--(dest) where ID(src) = %d return ID(dest) as ID" % x))

	if len(ys) != 0:
		prob_out = out / len(ys)

		for y in ys:
			r = y['ID']
			col[sparse_idx] = x
			row[sparse_idx] = r
			data[sparse_idx] = prob_out
			sparse_idx += 1

		data[sparse_idx] = prob_stay

	# No outgoing edge
	else:
		data[sparse_idx] = 1.0

	col[sparse_idx] = x
	row[sparse_idx] = x
	sparse_idx += 1

	if x % epoch == 0:
		print "Query epoch %d" % (x / epoch)


matrix = csc_matrix((data, (row, col)), shape=(num_node, num_node))

del row, col, data
gc.collect()

rank = np.full((num_node, 1), 1.0 / num_node)
print "Finish setting up page rank matrix"



for i in range(num_iter):
	rank = matrix.dot(rank)
	print rank
print "Finish iteration"

del matrix
gc.collect()

for i in range(num_node):
	session.run("match (n) where ID(n) = %d set n.pagerank = %f" %(i, rank[i] * 100.0))
	if i % epoch == 0:
	print "Insert epoch %d" % (i / epoch)

print "Finish updating page rank"


session.close()
