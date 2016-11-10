import io
import argparse
import itertools
import numpy as np
from gensim import utils
from gensim.models.word2vec import *
from gensim.models.doc2vec import *
from multiprocessing import Process
from nltk import word_tokenize, pos_tag
from nltk.stem.wordnet import WordNetLemmatizer
from neo4j.v1 import GraphDatabase, basic_auth
from scipy.spatial.distance import cosine


epoch = 50000
valid_POS = set(['NN', 'NNP', 'NNS', 'NNPS', 'JJ', 'JJR', 'JJS', 'VB', 'VBD', 'VBG', 'VBN', 'VBP', 'VBZ'])


def parse_args():
	parser = argparse.ArgumentParser()
	parser.add_argument('--phrase', nargs='?', default='../data/phrase.txt')
	parser.add_argument('--word2vec_corpus', nargs='?', default='../data/word2vec_corpus.txt')
	parser.add_argument('--doc2vec_corpus', nargs='?', default='../data/doc2vec_corpus.txt')
	parser.add_argument('--node2vec_corpus', nargs='?', default='../data/node2vec_corpus.txt')
	parser.add_argument('--word2vec_vector', nargs='?', default='../data/word2vec_vector.txt')
	parser.add_argument('--node2vec_vector', nargs='?', default='../data/node2vec_vector.txt')
	parser.add_argument('--dimension', nargs='?', default=64)
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
	with io.open(args.phrase, 'w', encoding='utf16') as f:
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
	with io.open(args.phrase, 'r', encoding='utf16') as f:
		for line in f:
			pair = line.rstrip().split(', ', 1)
			phrases[int(pair[0])] = pair[1]
	f.close()
	return phrases


def aggregate_phrases(session):
	print "Querying papers, researchers and conferences"
	num_node = session.run("match (n) return count(*) as count").single()['count']
	with io.open(args.doc2vec_corpus, 'w', encoding='utf16') as fd:
		with io.open(args.word2vec_corpus, 'w', encoding='utf16') as fw:
			for i in xrange(num_node / epoch + 1):
				lower = i * epoch
				upper = (i + 1) * epoch
				for paper in list(session.run("match (p:Paper) where ID(p) >= %d and ID(p) < %d return ID(p) as ID" % (lower, upper))):
					try:
						fd.write(str(paper['ID']) + ', ' + phrases[paper['ID']] + '\n')
						fw.write(phrases[paper['ID']] + '\n')
					except:
						pass

			min_researcher = session.run("match (r:Researcher) return min(ID(r)) as ID").single()['ID']
			max_researcher = session.run("match (r:Researcher) return max(ID(r)) as ID").single()['ID']		
			for i in xrange(min_researcher, max_researcher + 1, 1):
				title_buffer = ""
				for paper in list(session.run("match (r:Researcher)-->(p:Paper) where ID(r) = %d return ID(p) as ID" % i)):
					try:
						title_buffer += (phrases[paper['ID']] + ' ')
					except:
						pass
				if title_buffer != "":
					fd.write(str(i) + ', ' + title_buffer + '\n')

			min_conference = session.run("match (c:Conference) return min(ID(c)) as ID").single()['ID']
			max_conference = session.run("match (c:Conference) return max(ID(c)) as ID").single()['ID']
			for i in xrange(min_conference, max_conference + 1, 1):
				title_buffer = ""
				for paper in list(session.run("match (c:Conference)<--(p:Paper) where ID(c) = %d return ID(p) as ID" % i)):
					try:
						title_buffer += (phrases[paper['ID']] + ' ')
					except:
						pass
				if title_buffer != "":
					fd.write(str(i) + ', ' + title_buffer + '\n')
		fd.close()
	fw.close()


class LabeledLineSentence(object):
	def __init__(self, source):
		self.source = source
	def __iter__(self):
		for line in self.source:
			pair = line[:-1].split(', ')
			yield LabeledSentence(words=line[1].split(), tags=[line[0]])



def load_word2vec_sentence():
	return LineSentence(io.open(args.word2vec_corpus, 'r', encoding='utf-16'))

def load_doc2vec_sentence():
	return LabeledLineSentence(io.open(args.doc2vec_corpus, 'r', encoding='utf-16'))

def load_node2vec_sentence():
	return LineSentence(open(args.node2vec_corpus, 'r'))



class JointEmbedding(object):
	def __init__(self, num_process=8):
		self.word2vec_model = Word2Vec(size=args.dimension, window=4, min_count=1, workers=num_process, iter=1)
		self.node2vec_model = Word2Vec(size=args.dimension, window=2, min_count=1, workers=num_process, iter=1)
		self.doc2vec_model = Doc2Vec(size=args.dimension, window=4, min_count=1, workers=num_process, dm_concat=1, iter=1)
		self.word_vectors = {}
		self.node_vectors = {}
		self.build_vocab()
		self.fit()

	def build_vocab(self):
		print "Building vocabulary"
		self.word2vec_model.build_vocab(word2vec_sentence)
		self.node2vec_model.build_vocab(node2vec_sentence)
		self.doc2vec_model.build_vocab(doc2vec_sentence)

	def fit(self, iteration=20):
		print "Learning embedding"
		print "Iteration 0"
		self.word2vec(False)
		self.node2vec(False)
		for i in xrange(iteration - 1):
			self.doc2vec()
			print "Iteration %d" % (i + 1)
			self.word2vec(True)
			self.node2vec(True)
		self.doc2vec()
		self.save_vectors()

	def word2vec(self, prior):
		if prior:
			for word, vocab in iteritems(self.word2vec_model.vocab):
				self.word2vec_model.syn0[vocab.index] = self.word_vectors[word]
		self.word2vec_model.train(word2vec_sentence)
		for word, vocab in iteritems(self.word2vec_model.vocab):
   			self.word_vectors[word] = self.word2vec_model.syn0[vocab.index]


	def node2vec(self, prior):
		if prior:
			for node, vocab in iteritems(self.node2vec_model.vocab):
				self.node2vec_model.syn0[vocab.index] = self.node_vectors[node]
		self.node2vec_model.train(node2vec_sentence)
		for node, vocab in iteritems(self.node2vec_model.vocab):
   			self.node_vectors[node] = self.node2vec_model.syn0[vocab.index]

	def doc2vec(self):
		for word, vocab in iteritems(self.doc2vec_model.vocab):
			try:
				self.doc2vec_model.syn0[vocab.index] = self.word_vectors[word]
			except:
				pass
		for node in self.node_vectors:
			try:
				self.doc2vec_model.docvecs.doctag_syn0[self.doc2vec_model.docvecs._int_index(node)] = self.node_vectors[node]
			except:
				pass

		self.doc2vec_model.train(doc2vec_sentence)

		for word in self.word_vectors:
			try:
				self.word_vectors[word] = self.doc2vec_model[word]
			except:
				pass
		for node in self.node_vectors:
			try:
				self.node_vectors[node] = self.doc2vec_model.docvecs[node]
			except:
				pass

	def save_vectors(self, word2vec=False, node2vec=True):
		print "Saving vectors"
		if word2vec:
			with io.open(args.word2vec_vector, 'w', encoding='utf-16') as f:
				f.write(str(len(self.word_vectors)) + u' ' + str(args.dimension) + '\n')
				for word, vector in self.word_vectors.iteritems():
					f.write(word + ' ' + ' '.join(map(str, vector)) + '\n')
			f.close()

		if node2vec:
			with open(args.node2vec_vector, 'w') as f:
				f.write(str(len(self.node_vectors)) + ' ' + str(args.dimension) + '\n')
				for node, vector in self.node_vectors.iteritems():
					f.write(node + ' ' + ' '.join(map(str, vector)) + '\n')
			f.close()


def insert_vectors():
	print "Inserting vectors to db"
	with open(args.node2vec_vector, 'r') as f:
		next(f)
		for line in f:
			line = line[:-1].split(' ', 1)
			nodeID = int(line[0])
			vec = line[1]
			session.run("match (n) where ID(n) = %d set n.joint = '%s'" % (nodeID, vec))
	f.close()



driver = GraphDatabase.driver("bolt://localhost", auth = basic_auth("neo4j", "mliu60"))
session = driver.session()

args = parse_args()

# query_papers(session)
# phrases = load_phrases()
# aggregate_phrases(session)

# word2vec_sentence = load_word2vec_sentence()
# doc2vec_sentence = load_doc2vec_sentence()
# node2vec_sentence = load_node2vec_sentence()

# JointEmbedding()
insert_vectors()

session.close()


