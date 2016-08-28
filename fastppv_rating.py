import argparse
import gc
import time
import numpy as np
import networkx as nx
from multiprocessing import Process, Manager
from neo4j.v1 import GraphDatabase, basic_auth


epoch = 50000
random_walk_epoch = 10000
num_process = 4
num_rating = 10000


def parse_args():
	parser = argparse.ArgumentParser()

	parser.add_argument('--graph', nargs='?', default='karate.edgelist')

	parser.add_argument('--node', nargs='?', default='karate.node')

	parser.add_argument('--vector', nargs='?', default='karate.ppv')

	return parser.parse_args()


def construct_graph(session):
	print "Constructing graph from db"
	num_node = session.run("match (n) return count(*) as count").single()['count']
	IDs = set()
	with open(args.graph, 'w') as fg:
		with open(args.node, 'w') as fn:
			fn.write(str(num_node) + '\n')
			for i in range(num_node / epoch + 1):
				lower = i * epoch
				upper = (i+1) * epoch
				for edge in list(session.run("match (src)-->(dest) where ID(src) >= %d and ID(src) < %d "\
											 "return ID(src) as srcID, ID(dest) as destID, labels(src) as srcType, labels(dest) as destType" % (lower, upper))):
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



def read_graph():
	print "Reading graph"
	return nx.read_edgelist(args.graph, nodetype = int)


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
	for r in range(processID, num_node, num_process):
		if r % random_walk_epoch == 0:
			print "Random walk epoch: %d" % (r / random_walk_epoch)
		if nodes[r] != 'R':
			continue
		ego_G = nx.ego_graph(G, r, radius = 3, center = True, undirected = True)
		personalization = {n: 0.0 for n in ego_G.nodes()}
		personalization[r] = 1.0
		ranks = nx.pagerank_scipy(ego_G, alpha = 0.9, personalization = personalization, tol = 1e-02,  max_iter = 64)
		del ego_G
		del personalization
		gc.collect()
		ranks = np.array(ranks.items())
		ranks[:,1] *= 1e6
		ranks = ranks.astype(int)
		ranks = np.array(filter(lambda row: nodes[row[0]] == 'P' and row[1] > 0, ranks))
		for rank in ranks:
			ratings.append((r, rank[0], rank[1]))



driver = GraphDatabase.driver("bolt://localhost", auth = basic_auth("neo4j", "mliu60"))
session = driver.session()

args = parse_args()
open(args.vector, 'w').close()

construct_graph(session)

manager = Manager()
G = read_graph()
num_node, nodes = read_nodes()
ratings = manager.list()

start_time = time.time()

processes = []
for j in range(num_process):
	p = Process(target = full_ratings, args = (j, G, num_node, nodes, ratings, ))
	processes.append(p)
	p.start()

for p in processes:
	p.join()

print("--- %d seconds ---" % (time.time() - start_time))

np.savetxt(args.rating, ratings, fmt='%d', delimiter = ',')

session.close()

