import numpy as np
import networkx as nx
import random


class Graph():
	def __init__(self, nx_G, is_directed, p, q):
		assert not is_directed
		assert p == 1 and q == 1
		self.G = nx_G
		self.is_directed = is_directed
		self.p = p
		self.q = q

	def node2vec_walk(self, walk_length, start_node):
		'''
		Simulate a random walk starting from start node.
		'''
		G = self.G
		alias_nodes = self.alias_nodes

		walk = [start_node]

		while len(walk) < walk_length:
			cur = walk[-1]
			cur_nbrs = sorted(G.neighbors(cur))
			if len(cur_nbrs) > 0:
				walk.append(cur_nbrs[alias_draw(alias_nodes[cur][0], alias_nodes[cur][1])])
			else:
				break

		return walk

	def simulate_walks(self, num_walks, walk_length, filename):
		'''
		Repeatedly simulate random walks from each node.
		'''
		G = self.G
		nodes = list(G.nodes())
		print 'Walk iteration:'
		with open(filename, 'w') as f:
			for walk_iter in range(num_walks):
				print str(walk_iter+1), '/', str(num_walks)
				random.shuffle(nodes)
				for node in nodes:
					if walk_iter <= G.degree(node):
						walk = self.node2vec_walk(walk_length=walk_length, start_node=node)
						f.write(' '.join(map(str, walk)) + '\n')
		f.close()
		return

	def preprocess_transition_probs(self):
		'''
		Preprocessing of transition probabilities for guiding the random walks.
		'''
		print "Preprocessing transitions probabilities"
		G = self.G

		alias_nodes = {}

		for node in G.nodes():
			neighbors = G.neighbors(node)
			unnormalized_probs = [G[node][nbr]['weight'] for nbr in sorted(neighbors)]
			norm_const = sum(unnormalized_probs)
			normalized_probs =  [float(u_prob)/norm_const for u_prob in unnormalized_probs]
			alias_nodes[node] = alias_setup(normalized_probs)

		self.alias_nodes = alias_nodes

		return


def alias_setup(probs):
	'''
	Compute utility lists for non-uniform sampling from discrete distributions.
	Refer to https://hips.seas.harvard.edu/blog/2013/03/03/the-alias-method-efficient-sampling-with-many-discrete-outcomes/
	for details
	'''
	K = len(probs)
	q = np.zeros(K)
	J = np.zeros(K, dtype=np.int)

	smaller = []
	larger = []
	for kk, prob in enumerate(probs):
		q[kk] = K*prob
		if q[kk] < 1.0:
			smaller.append(kk)
		else:
			larger.append(kk)

	while len(smaller) > 0 and len(larger) > 0:
		small = smaller.pop()
		large = larger.pop()

		J[small] = large
		q[large] = q[large] + q[small] - 1.0
		if q[large] < 1.0:
			smaller.append(large)
		else:
			larger.append(large)

	return J, q

def alias_draw(J, q):
	'''
	Draw sample from a non-uniform discrete distribution using alias sampling.
	'''
	K = len(J)

	kk = int(np.floor(np.random.rand()*K))
	if np.random.rand() < q[kk]:
		return kk
	else:
		return J[kk]
		