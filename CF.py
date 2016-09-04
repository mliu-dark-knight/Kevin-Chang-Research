from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating

conf = SparkConf().setAppName("collaborative filtering")
sc = SparkContext(conf = conf)

# Load and parse the data
data = sc.textFile("FastPPV/dblp/vectors/fastppv-pagerank_100000_8")
ratings = data.map(lambda l: l.split(' ')).map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))

# Build the recommendation model using Alternating Least Squares
rank = 16
numIterations = 16
model = ALS.train(ratings, rank, numIterations)
print "Finish training ALS"

Evaluate the model on training data
testdata = ratings.map(lambda p: (p[0], p[1]))
predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
print("Mean Squared Error = " + str(MSE))

with open("karate2010.ppv", 'w') as f:
	for (idx, vec) in model.userFeatures().collect():
		f.write(str(idx) + ' ' + ' '.join(map(str, vec)) + '\n')
	for (idx, vec) in model.productFeatures().collect():
		f.write(str(idx) + ' ' + ' '.join(map(str, vec)) + '\n')