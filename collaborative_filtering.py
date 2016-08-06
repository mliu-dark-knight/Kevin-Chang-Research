import argparse
import numpy as np
import networkx as nx
from multiprocessing import Process, Manager
from neo4j.v1 import GraphDatabase, basic_auth


epoch = 500
num_processor = 16


def parse_args():
	'''
	Parses the node2vec arguments.
	'''
	parser = argparse.ArgumentParser()

	parser.add_argument('--graph', nargs='?', default='graph.txt')

	parser.add_argument('--node', nargs='?', default='node.txt')

	parser.add_argument('--rating', nargs='?', default='rating.txt')

	parser.add_argument('--vector', nargs='?', default='vector.txt')

	parser.add_argument('--dimensions', type=int, default=64)

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
											 "return ID(src) as srcID, labels(src) as srcType, ID(dest) as destID, labels(dest) as destType" % (lower, upper))):
					srcID, destID = edge['srcID'], edge['destID']
					srcType, destType = edge['srcType'][0], edge['destType'][0]
					fg.write(str(srcID) + ' ' + str(destID) + '\n')
					if srcID not in IDs:
						IDs.add(srcID)
						fn.write(str(srcID) + ' ' + srcType[0] + '\n')
					if destID not in IDs:
						IDs.add(destID)
						fn.write(str(destID) + ' ' + destType[0] + '\n')

				break
		fn.close()
	fg.close()



def read_graph():
	print "Reading graph"
	return nx.read_edgelist(args.graph, nodetype = int, create_using = nx.DiGraph()).to_undirected()


def read_nodes():
	print "Reading nodes"
	with open(args.node, 'r') as f:
		num_node = int(f.readline())
		nodes = charar = np.chararray(num_node)
		nodes[:] = 'N'
		for line in f:
			[nID, nType] = line[:-1].split(' ')
			assert nID != None and nType != None
			nodes[int(nID)] = nType
	f.close()
	return nodes


def create_ratings(processorID, G, nodes):
	print "Processor: %d" % processorID
	pass




driver = GraphDatabase.driver("bolt://localhost", auth = basic_auth("neo4j", "mliu60"))
session = driver.session()

args = parse_args()
open(args.vector, 'w').close()

# construct_graph(session)

manager = Manager()
G = read_graph()
nodes = read_nodes()

processes = []
for i in range(num_processor):
	p = Process(target = create_ratings, args = (i, G, nodes, ))
	processes.append(p)
	p.start()

for p in processes:
	p.join()


session.close()

