'''
Reference implementation of node2vec. 
Author: Aditya Grover
For more details, refer to the paper:
node2vec: Scalable Feature Learning for Networks
Aditya Grover and Jure Leskovec 
Knowledge Discovery and Data Mining (KDD), 2016
'''

import argparse
import csv
import numpy as np
import networkx as nx
import node2vec
from gensim.models import Word2Vec
from neo4j.v1 import GraphDatabase, basic_auth


def parse_args():
	'''
	Parses the node2vec arguments.
	'''
	parser = argparse.ArgumentParser(description="Run node2vec.")

	parser.add_argument('--input', nargs='?', default='karate.edgelist',
	                    help='Input graph path')

	parser.add_argument('--output', nargs='?', default='karate.emb',
	                    help='Embeddings path')

	parser.add_argument('--dimensions', type=int, default=128,
	                    help='Number of dimensions. Default is 128.')

	parser.add_argument('--walk-length', type=int, default=10,
	                    help='Length of walk per source. Default is 10.')

	parser.add_argument('--num-walks', type=int, default=10,
	                    help='Number of walks per source. Default is 40.')

	parser.add_argument('--window-size', type=int, default=10,
                    	help='Context size for optimization. Default is 10.')

	parser.add_argument('--iter', default=1, type=int,
                      help='Number of epochs in SGD')

	parser.add_argument('--workers', type=int, default=8,
	                    help='Number of parallel workers. Default is 8.')

	parser.add_argument('--p', type=float, default=1,
	                    help='Return hyperparameter. Default is 1.')

	parser.add_argument('--q', type=float, default=1,
	                    help='Inout hyperparameter. Default is 1.')

	parser.add_argument('--weighted', dest='weighted', action='store_true',
	                    help='Boolean specifying (un)weighted. Default is weighted.')
	parser.add_argument('--unweighted', dest='unweighted', action='store_false')
	parser.set_defaults(weighted=True)

	parser.add_argument('--directed', dest='directed', action='store_true',
	                    help='Graph is (un)directed. Default is undirected.')
	parser.add_argument('--undirected', dest='undirected', action='store_false')
	parser.set_defaults(directed=False)

	return parser.parse_args()

def read_graph():
	'''
	Reads the input network in networkx.
	'''
	print "Reading graph"
	if args.weighted:
		G = nx.read_edgelist(args.input, nodetype=int, data=(('weight',float),), create_using=nx.DiGraph())
	else:
		G = nx.read_edgelist(args.input, nodetype=int, create_using=nx.DiGraph())
		for edge in G.edges():
			G[edge[0]][edge[1]]['weight'] = 1

	if not args.directed:
		G = G.to_undirected()

	return G

def learn_embeddings(walks):
	'''
	Learn embeddings by optimizing the Skipgram objective using SGD.
	'''
	print "Learning embeddings"
	walks = [map(str, walk) for walk in walks]
	model = Word2Vec(walks, size=args.dimensions, window=args.window_size, min_count=0, 
		workers=args.workers, iter=args.iter)
	model.save_word2vec_format(args.output)
	
	return

def create_input():
	print "Creating input from db"
	with open(args.input, 'w') as f:
		for edge in list(session.run("match (src)-->(dest) return ID(src) as srcID, ID(dest) as destID, src.pagerank as srcR, dest.pagerank as destR")):
			srcID = edge['srcID']
			destID = edge['destID']
			srcR = edge['srcR']
			destR = edge['destR']
			edgeR = srcR + destR
			if edgeR == 0:
				edgeR = 10**(-6)
			f.write(str(srcID) + ' ' + str(destID) + ' ' + str(edgeR) + '\n')
	f.close()


def save_output():
	print "Saving output to db"
	with open(args.output, 'r') as f:
		for line in f:
			line = line[:-1].split(' ', 1)
			nodeID = int(line[0])
			vec = (', ').join(line[1].split(' '))
			session.run("match (n) where ID(n) = %d set n.vector = '%s'" % (nodeID, vec))
	f.close()

def main(args):
	'''
	Pipeline for representational learning for all nodes in a graph.
	'''
	create_input()
	nx_G = read_graph()
	G = node2vec.Graph(nx_G, args.directed, args.p, args.q)
	G.preprocess_transition_probs()
	walks = G.simulate_walks(args.num_walks, args.walk_length)
	learn_embeddings(walks)
	save_output()


driver = GraphDatabase.driver("bolt://localhost", auth = basic_auth("neo4j", "mliu60"))
session = driver.session()
args = parse_args()
main(args)
session.close()

