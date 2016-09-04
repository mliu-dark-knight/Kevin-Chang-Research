import argparse
import gc
import time
import numpy as np
from igraph import Graph
from multiprocessing import Process, Manager
from neo4j.v1 import GraphDatabase, basic_auth


epoch = 50000
random_walk_epoch = 50000
num_process = 4
num_rating = 10000


def parse_args():
	parser = argparse.ArgumentParser()

	parser.add_argument('--edge', nargs='?', default='karate2010.edgelist')

	parser.add_argument('--node', nargs='?', default='karate2010.node')

	parser.add_argument('--rating', nargs='?', default='karate2010.rating')

	parser.add_argument('--vector', nargs='?', default='karate2010.ppv')

	return parser.parse_args()


def construct_graph(session):
	print "Constructing graph from db"
	num_node = session.run("match (n) return count(*) as count").single()['count']
	IDs = set()
	with open(args.edge, 'w') as fg:
		with open(args.node, 'w') as fn:
			fn.write(str(num_node) + '\n')
			for i in xrange(num_node / epoch + 1):
				lower = i * epoch
				upper = (i+1) * epoch
				for query in ["match (r:Researcher)-->(p:Paper) where p.year >= 2010 and ID(r) >= %d and ID(r) < %d "\
							  "return ID(r) as srcID, ID(p) as destID, labels(r) as srcType, labels(p) as destType" % (lower, upper), 
							  "match (p:Paper)-->(c:Conference) where p.year >= 2010 and ID(p) >= %d and ID(p) < %d "\
							  "return ID(p) as srcID, ID(c) as destID, labels(p) as srcType, labels(c) as destType" % (lower, upper), 
							  "match (p1:Paper)-->(p2:Paper) where p1.year >= 2010 and p2.year >= 2010 and ID(p1) >= %d and ID(p1) < %d "\
							  "return ID(p1) as srcID, ID(p2) as destID, labels(p1) as srcType, labels(p2) as destType" % (lower, upper)]:
					for edge in list(session.run(query)):
						srcID, destID = edge['srcID'], edge['destID']
						srcType, destType = edge['srcType'][0], edge['destType'][0]
						fg.write(str(srcID) + ' ' + str(destID) + '\n')
						if srcID not in IDs:
							IDs.add(srcID)
							fn.write(str(srcID) + ' ' + srcType[0] + '\n')
						if destID not in IDs:
							IDs.add(destID)
							fn.write(str(destID) + ' ' + destType[0] + '\n')

		fn.close()
	fg.close()



def read_nodes():
	print "Reading nodes"
	with open(args.node, 'r') as f:
		num_node = int(f.readline())
		nodes = np.chararray(num_node)
		nodes[:] = 'N'
		for line in f:
			[nID, nType] = line[:-1].split(' ')
			assert nID != None and nType != None
			nodes[int(nID)] = nType
	f.close()
	return num_node, nodes


def full_ratings(processID, G, num_node, nodes, ratings):
	print "Creating ratings"
	for r in xrange(processID, num_node, num_process):
		if r % random_walk_epoch == 0:
			print "Random walk epoch: %d" % (r / random_walk_epoch)
		if nodes[r] != 'R':
			continue
		ranks = G.personalized_pagerank(directed = False, damping = 0.9, weights = None, reset_vertices = r)
		ranks = np.array(ranks)
		ranks *= 1e6
		ranks = ranks.astype(int)
		ranks = np.array(filter(lambda row: nodes[row[0]] == 'P' and row[1] > 0, np.column_stack((np.arange(len(ranks)), ranks))))
		for rank in ranks:
			ratings.append((r, rank[0], rank[1]))


def save_vectors():
	print "Saving vectors to db"
	with open(args.vector, 'r') as f:
		i = 0
		for line in f:
			line = line[:-1].split(' ', 1)
			nodeID = int(line[0])
			vec = line[1]
			session.run("match (n) where ID(n) = %d set n.node2vec = '%s'" % (nodeID, vec))
			if i % epoch == 0:
				print "Insert epoch %d" % (i / epoch)
			i += 1

	f.close()



driver = GraphDatabase.driver("bolt://localhost", auth = basic_auth("neo4j", "mliu60"))
session = driver.session()

args = parse_args()
open(args.vector, 'w').close()

construct_graph(session)

manager = Manager()
G = Graph.Read_Ncol(args.edge, directed = False)
num_node, nodes = read_nodes()
ratings = manager.list()

start_time = time.time()

processes = []
for j in xrange(num_process):
	p = Process(target = full_ratings, args = (j, G, num_node, nodes, ratings, ))
	processes.append(p)
	p.start()

for p in processes:
	p.join()

print("--- %d seconds ---" % (time.time() - start_time))

np.savetxt(args.rating, ratings, fmt='%d', delimiter = ',')

save_vectors()

session.close()

