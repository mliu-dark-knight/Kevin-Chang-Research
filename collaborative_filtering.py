import numpy as np


associationIdx = None
associationRating = None
vectors = None

class CF(object):
	def __init__(self, nodeFile, ratingFile):
		self.nodeFile = nodeFile
		self.ratingFile = ratingFile
		self.associationIdx = None
		self.associationRating = None
		self.vectors = None

	def initialize(self):
		with open(self.nodeFile, 'r') as f:
			num_node = int(f.readline()[:-1])
		f.close()
		self.associationIdx = np.empty([num_node], list)
		self.associationRating = np.empty([num_node], list)
		self.vectors = np.random.rand(num_node, 64)
		with open(self.ratingFile, 'r') as f:
			for line in f:
				pair = line[:-1].split(' ')
				researcher, paper, rating = int(pair[0]), int(pair[1]), float(pair[2])
				if self.associationIdx[researcher] is None:
					self.associationIdx[researcher] = []
					self.associationRating[researcher] = []
				self.associationIdx[researcher].append(paper)
				self.associationRating[researcher].append(rating)
				if self.associationIdx[paper] is None:
					self.associationIdx[paper] = []
					self.associationRating[paper] = []
				self.associationIdx[paper].append(researcher)
				self.associationRating[paper].append(researcher)
		f.close()
		print "Finish initialization"


	def fit(self):
		pass


	def writeToFile(self, vectorFile):
		with open(vectorFile, 'w') as f:
			for i in range(len(self.vectors)):
				f.write(str(i) + ' ' + str(self.vectors[i]))
		f.close()


