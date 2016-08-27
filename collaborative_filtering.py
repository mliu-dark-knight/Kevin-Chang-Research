import numpy as np

def parse_args():
	parser.add_argument('--dimensions', type = int, default = 64)

	parser.add_argument('--Iter', default = 20, type = int)

	parser.add_argument('--Lambda', default = 0.1, type = float)

	parser.add_argument('--vector', nargs='?', default='karate.ppv')


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

	def initialize(self):
		with open(self.ratingFile, 'r') as f:
			self.num_node = int(f.readline()[:-1])
		f.close()
		self.associationIdx = np.empty([num_node], list)
		self.associationRating = np.empty([num_node], list)
		self.vectors = np.random.rand(num_node, self.dimensions)
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
			gradients = np.zeros((self.num_node, self.dimensions), dtype = np.float)
			for i in range(self.num_node):
				idxList, ratingList = self.associationIdx[i], self.associationRating[i]
				for j in range(len(idxList)):
					gradients[i] += ((np.dot(self.vectors[i], self.vectors[idxList[j]]) - ratingList[j]) * self.vectors[idxList[j]] + 
									self.learningRate(epoch) * self.vectors[i])
			for i in range(self.num_node):
				self.vectors[i] -= gradients[i]


	def learningRate(self, epoch):
		return 1.0 / (1.0 + epoch)
		

	def writeToFile(self, vectorFile):
		with open(vectorFile, 'w') as f:
			for i in range(len(self.vectors)):
				f.write(str(i) + ' ' + str(self.vectors[i]))
		f.close()



def main():
	collaborative_filtering = CF(args.dimensions, args.iter, args.Lambda, None)
	collaborative_filtering.writeToFile(args.vector)

args = parse_args()
main()

