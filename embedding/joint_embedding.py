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
num_process = 8
iteration = 20

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
	with io.open(args.word2vec_corpus, 'r', encoding='utf16') as f:
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
				for title in list(session.run("match (p:Paper) where ID(p) >= %d and ID(p) < %d return ID(p) as ID, p.title as title" % (lower, upper))):
					phrase = extract_phrases(title['title'])
					if phrase != []:
						fd.write(str(title['ID']) + ', ' + ' '.join(phrase) + '\n')
						fw.write(' '.join(phrase) + '\n')

				min_researcher = session.run("match (r:Researcher) return min(ID(r)) as ID").single()['ID']
				max_researcher = session.run("match (r:Researcher) return max(ID(r)) as ID").single()['ID']		

			for i in xrange(min_researcher, max_researcher + 1, 1):
				title_buffer = ""
				for paper in list(session.run("match (r:Researcher)--(p:Paper) where ID(r) = %d return ID(p) as ID" % i)):
					title_buffer += (phrases[paper['ID']] + ' ')
				if title_buffer != "":
					fd.write(str(i) + ', ' + title_buffer + '\n')
					fw.write(title_buffer + '\n')

			min_conference = session.run("match (c:Conference) return min(ID(c)) as ID").single()['ID']
			max_conference = session.run("match (c:Conference) return max(ID(c)) as ID").single()['ID']
			for i in xrange(min_conference, max_conference + 1, 1):
				title_buffer = ""
				for paper in list(session.run("match (c:Conference)--(p:Paper) where ID(c) = %d return ID(p) as ID" % i)):
					title_buffer += (phrases[paper['ID']] + ' ')
				if title_buffer != "":
					fd.write(str(i) + ', ' + title_buffer + '\n')
					fw.write(title_buffer + '\n')
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
	sentences = []
	with io.open(args.doc2vec_corpus, 'r', encoding='utf-16') as f:
		for line in f:
			pair = line[:-1].split(', ')
			sentences.append(LabeledSentence(words=pair[1].split(), tags=[pair[0]]))
	f.close()
	return sentences

def load_node2vec_sentence():
	return LineSentence(open(args.node2vec_corpus, 'r'))



def embedding(corpus=None, vectors=None, prior=None, window=None, fname=None):
	if prior:
		model = Word2Vec(size=args.dimension, window=window, min_count=1, workers=num_process, iter=1)
		model.build_vocab(corpus)
		for word, vocab in sorted(iteritems(model.vocab), key=lambda item: -item[1].count):
			model.syn0[vocab.index] = vectors[word]
		model.train(corpus)
	else:
		model = Word2Vec(sentences=corpus, size=args.dimension, window=window, min_count=1, workers=num_process, iter=1)

	for word, vocab in sorted(iteritems(model.vocab), key=lambda item: -item[1].count):
   		vectors[word] = model.syn0[vocab.index]

	if fname:
		model.save_word2vec_format(fname)


def word2vec(prior):
	embedding(corpus=word2vec_sentence, vectors=word_vectors, prior=prior, window=4)


def node2vec(prior):
	embedding(corpus=node2vec_sentence, vectors=node_vectors, prior=prior, window=2)



def doc2vec():
	model = Doc2Vec(size=64, window=4, min_count=1, workers=num_process, dm_concat=1, iter=1)
	model.build_vocab(doc2vec_sentence)

	# load word vectors
	for i in xrange(len(model.syn0)):
		try:
			model.syn0[i] = word_vectors[model.index2word[i]]
		except:
			pass

	# load node vectors
	for node in node_vectors:
		model.docvecs.doctag_syn0[model.docvecs._int_index(node)] = node_vectors[node]

	model.train(doc2vec_sentence)

	# save word vectors
	for word in word_vectors:
		word_vectors[word] = model[word]

	# save node vectors
	for node in node_vectors:
		node_vectors[node] = model.docvecs[node]


def save_vectors(word2vec=False, node2vec=True):
	if word2vec:
		with io.open(args.word2vec_vector, 'w', encoding='utf-16') as f:
			f.write(str(len(word_vectors)) + u' ' + str(args.dimension) + '\n')
			for word, vector in word_vectors.iteritems():
				f.write(word + ' ' + ' '.join(map(str, vector)) + '\n')
		f.close()

	if node2vec:
		with open(args.node2vec_vector, 'w') as f:
			f.write(str(len(node_vectors)) + ' ' + str(args.dimension) + '\n')
			for node, vector in node_vectors.iteritems():
				f.write(node + ' ' + ' '.join(map(str, vector)) + '\n')
		f.close()


def fit():
	print "Learning embedding"
	word2vec(False)
	node2vec(False)
	for i in xrange(iteration - 1):
		doc2vec()
		print "Iteration %d" % i
		word2vec(True)
		node2vec(True)
	doc2vec()
	print "Iteration" % i
	save_vectors()


driver = GraphDatabase.driver("bolt://localhost", auth = basic_auth("neo4j", "mliu60"))
session = driver.session()

args = parse_args()

query_papers(session)
phrases = load_phrases()
aggregate_phrases(session)

# word_vectors = {}
# node_vectors = {}

# word2vec_sentence = load_word2vec_sentence()
# doc2vec_sentence = load_doc2vec_sentence()
# node2vec_sentence = load_node2vec_sentence()

# fit()

session.close()


