import argparse
import numpy as np

def parse_args():
	parser = argparse.ArgumentParser()
	parser.add_argument('--dimensions', type = int, default = 16)
	parser.add_argument('--Iter', default = 16, type = int)
	parser.add_argument('--Lambda', default = 1.0, type = float)
	parser.add_argument('--vector', nargs='?', default='karate.ppv')
	return parser.parse_args()


class CF(object):
	def __init__(self, dimensions, Iter, Lambda, ratingFile):
		self.dimensions = dimensions
		self.Iter = Iter
		self.Lambda = Lambda
		self.ratingFile = ratingFile
		self.num_node = None
		self.userList = []
		self.itemList = []
		self.associationIdx = None
		self.associationRating = None
		self.vectors = None
		self.initialize()


	def initialize(self):
		with open(self.ratingFile, 'r') as f:
			self.num_node = int(f.readline()[:-1])
		f.close()
		self.associationIdx = np.empty([self.num_node], list)
		self.associationRating = np.empty([self.num_node], list)
		self.vectors = np.random.rand(self.num_node, self.dimensions)
		self.vectors *= 1e2
		with open(self.ratingFile, 'r') as f:
			next(f)
			for line in f:
				pair = line[:-1].split(' ')
				user, item, rating = int(pair[0]), int(pair[1]), float(pair[2])
				if self.associationIdx[user] is None:
					self.associationIdx[user] = []
					self.associationRating[user] = []
					self.userList.append(user)
				self.associationIdx[user].append(item)
				self.associationRating[user].append(rating)
				if self.associationIdx[item] is None:
					self.associationIdx[item] = []
					self.associationRating[item] = []
					self.itemList.append(item)
				self.associationIdx[item].append(user)
				self.associationRating[item].append(user)
		f.close()
		print "Finish initialization"


	def fit(self):
		for epoch in range(self.Iter):
			print "Iteration %d" % epoch
			gradients = np.empty((self.num_node, self.dimensions))
			for i in range(self.num_node):
				gradients[i] = self.Lambda * self.vectors[i]
			for i in range(self.num_node):
				idxList, ratingList = self.associationIdx[i], self.associationRating[i]
				if idxList is not None or ratingList is not None:
					for j in range(len(idxList)):
						gradients[i] += ((np.dot(self.vectors[i], self.vectors[idxList[j]]) - ratingList[j]) * self.vectors[idxList[j]])
			print gradients
			self.vectors -= self.learningRate(epoch) * gradients
		print "Finish gradient descent"


	def validNode(self, i):
		return self.associationIdx[i] is not None and self.associationRating[i] is not None


	def learningRate(self, epoch):
		return 1e-7 / (1 + epoch / 1.25)
		

	def writeToFile(self, vectorFile):
		with open(vectorFile, 'w') as f:
			for i in range(self.num_node):
				if self.validNode(i):
					f.write(str(i) + ' ' + str(' '.join(map(str, self.vectors[i]))) + '\n')
		f.close()



def main():
	collaborative_filtering = CF(args.dimensions, args.Iter, args.Lambda, "FastPPV/dblp/vectors/fastppv-pagerank_100000_3")
	collaborative_filtering.fit()
	collaborative_filtering.writeToFile(args.vector)

args = parse_args()
main()

