import io
import gc
import argparse
import fcntl
import numpy as np
from multiprocessing import Process
from gensim import corpora, models
from scipy.spatial.distance import cosine
from nltk import word_tokenize, pos_tag
from nltk.stem.wordnet import WordNetLemmatizer
from neo4j.v1 import GraphDatabase, basic_auth

epoch = 50000
valid_POS = set(['NN', 'NNP', 'NNS', 'NNPS', 'JJ', 'JJR', 'JJS'])
num_topics = 64
num_processes = 8


def parse_args():
	parser = argparse.ArgumentParser()
	parser.add_argument('--phrase', nargs='?', default='data/phrase.txt')
	parser.add_argument('--corpus', nargs='?', default='data/corpus.txt')
	parser.add_argument('--inference', nargs='?', default='data/inference.txt')
	parser.add_argument('--dictionary', nargs='?', default='model/LDA.dict')
	parser.add_argument('--model', nargs='?', default='model/LDA.model')
	parser.add_argument('--vector', nargs='?', default='data/LDA.txt')
	parser.add_argument('--graph', nargs='?', default='data/karate.edgelist')
	return parser.parse_args()


def extract_phrases(text):
	lmtzr = WordNetLemmatizer()
	token_buffer = []
	tokens = word_tokenize(text.lower())
	annotation = pos_tag(tokens)
	for (token, pos) in annotation:
		if pos in valid_POS:
			if len(lmtzr.lemmatize(token)) > 1:
				token_buffer.append(lmtzr.lemmatize(token))
	return token_buffer


def query_papers(session):
	print "Querying papers"
	num_node = session.run("match (n) return count(*) as count").single()['count']
	with io.open(args.phrase, 'w', encoding='utf-16') as f:
		for i in xrange(num_node / epoch + 1):
			lower = i * epoch
			upper = (i + 1) * epoch
			for title in list(session.run("match (p:Paper) where ID(p) >= %d and ID(p) < %d return ID(p) as ID, p.title as title" % (lower, upper))):
				phrase = extract_phrases(title['title'])
				if phrase != []:
					f.write(str(title['ID']) + ', ' + u' '.join(phrase) + '\n')
	f.close()


def load_phrases():
	print "Loading phrases"
	phrases = {}
	with io.open(args.phrase, 'r', encoding='utf-16') as f:
		for line in f:
			pair = line.rstrip().split(', ', 1)
			phrases[int(pair[0])] = pair[1]
	f.close()
	return phrases


def aggregate_phrases(session):
	print "Querying papers, researchers and conferences"
	num_node = session.run("match (n) return count(*) as count").single()['count']
	with io.open(args.corpus, 'w', encoding='utf-16') as fc:
		with io.open(args.inference, 'w', encoding='utf-16') as fi:
			for i in xrange(num_node / epoch + 1):
				lower = i * epoch
				upper = (i + 1) * epoch
				for paper in list(session.run("match (p:Paper) where ID(p) >= %d and ID(p) < %d return ID(p) as ID" % (lower, upper))):
					try:
						fi.write(str(paper['ID']) + ', ' + phrases[paper['ID']] + '\n')
					except:
						pass
			min_researcher = session.run("match (r:Researcher) return min(ID(r)) as ID").single()['ID']
			max_researcher = session.run("match (r:Researcher) return max(ID(r)) as ID").single()['ID']		
			for i in xrange(min_researcher, max_researcher + 1, 1):
				title_buffer = ""
				for paper in list(session.run("match (r:Researcher)--(p:Paper) where ID(r) = %d return ID(p) as ID" % i)):
					try:
						title_buffer += (phrases[paper['ID']] + ' ')
					except:
						pass
				if title_buffer != "":
					try:
						fc.write(title_buffer + '\n')
						fi.write(str(i) + ', ' + title_buffer + '\n')
					except:
						pass
			min_conference = session.run("match (c:Conference) return min(ID(c)) as ID").single()['ID']
			max_conference = session.run("match (c:Conference) return max(ID(c)) as ID").single()['ID']
			for i in xrange(min_conference, max_conference + 1, 1):
				title_buffer = ""
				for paper in list(session.run("match (c:Conference)--(p:Paper) where ID(c) = %d return ID(p) as ID" % i)):
					try:
						title_buffer += (phrases[paper['ID']] + ' ')
					except:
						pass
				if title_buffer != "":
					try:
						fi.write(str(i) + ', ' + title_buffer + '\n')
					except:
						pass
		fi.close()
	fc.close()


def build_dictionary():
	print "Building dictionary"
	dictionary = corpora.Dictionary(phrases)
	dictionary.save(args.dictionary)


def learn_LDA():
	print "Learning LDA"
	texts = []
	with io.open(args.corpus, 'r', encoding='utf-16') as f:
		for line in f:
			texts.append(line[:-1].split(' '))
	f.close()

	dictionary = corpora.Dictionary.load(args.dictionary)
	corpus = [dictionary.doc2bow(text) for text in texts]
	del texts
	gc.collect()

	ldamodel = models.ldamulticore.LdaMulticore(corpus, num_topics=num_topics, id2word=dictionary, passes=20, workers=4)
	ldamodel.save(args.model)


def inference_singlecore(pid, dictionary, ldamodel):
	with io.open(args.inference, 'r', encoding='utf-16') as fi:
		counter = 0
		for line in fi:
			if counter % num_processes == pid:
				pair = line[:-1].split(', ')
				topic = np.zeros(num_topics)
				for (key, val) in ldamodel[dictionary.doc2bow(pair[1].split())]:
					topic[key] = val
				fv = open(args.vector, 'a')
				fv.write(str(pair[0]) + ', ' + ' '.join(map(str, topic)) + '\n')
				fv.close()
			counter += 1
	fi.close()


def inference_multicore():
	print "Inferencing"
	dictionary = corpora.Dictionary.load(args.dictionary)

	ldamodel = models.LdaModel.load(args.model, mmap='r')
	ldamodel.random_state = np.random.mtrand._rand

	processes = []
	for j in xrange(num_processes):
		p = Process(target=inference_singlecore, args=(j, dictionary, ldamodel, ))
		processes.append(p)
		p.start()
	for p in processes:
		p.join()


def topic_terms():
	dictionary = corpora.Dictionary.load(args.dictionary)
	ldamodel = models.LdaModel.load(args.model, mmap='r')
	for i in xrange(num_topics):
		print '********'
		print 'Topic %d' % i
		for (id, prob) in ldamodel.get_topic_terms(i, topn=1):
			print dictionary.get(id), prob


def insert_vectors(session):
	print "Saving vectors to db"
	with open(args.vector, 'r') as f:
		for line in f:
			pair = line[:-1].split(', ', 1)
			session.run("match (n) where ID(n) = %d set n.LDA = '%s'" % (int(pair[0]), pair[1]))
	f.close()


def load_vectors():
	print "Loading vectors"
	vectors = {}
	with open(args.vector, 'r') as f:
		for line in f:
			pair = line[:-1].split(', ')
			vectors[int(pair[0])] = np.array(list(map(float, pair[1].split())))
	f.close()
	return vectors


def assign_weight(session):
	print "Assigning weight to relationships"
	num_node = session.run("match (n) return count(*) as count").single()['count']
	with open(args.graph, 'r') as f:
		for line in f:
			pair = line[:-1].split()
			srcID, destID = int(pair[0]), int(pair[1])
			try:
				srcVec, destVec = vectors[srcID], vectors[destID]
				cos = 1.0 - cosine(srcVec, destVec)
				session.run("match (src)-[r]->(dest) where ID(src) = %d and ID(dest) = %d set r.weight = %f" % (srcID, destID, cos))
			except:
				pass
	f.close()



driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "mliu60"))
session = driver.session()

args = parse_args()

query_papers(session)
phrases = load_phrases()
aggregate_phrases(session)
build_dictionary()
learn_LDA()
inference_multicore()
topic_terms()
insert_vectors(session)
vectors = load_vectors()
assign_weight(session)

session.close()


