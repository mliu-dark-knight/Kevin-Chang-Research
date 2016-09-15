import json
import io
import argparse
import numpy as np
from gensim.models import Doc2Vec
from gensim.models.doc2vec import LabeledSentence
from nltk import word_tokenize, pos_tag
from neo4j.v1 import GraphDatabase, basic_auth

epoch = 50000
valid_POS = set(['NN', 'NNP', 'NNS', 'NNPS', 'JJ', 'JJR', 'JJS'])

def parse_args():
	parser = argparse.ArgumentParser()
	parser.add_argument('--title', nargs='?', default='title.txt')
	parser.add_argument('--phrase', nargs='?', default='phrase.txt')
	parser.add_argument('--vector', nargs='?', default='doc2vec.txt')
	return parser.parse_args()


def extract_phrases(text):
	print "Extracting phrases"
	token_buffer = []
	tokens = word_tokenize(text)
	annotation = pos_tag(tokens)
	for (token, pos) in annotation:
		if pos in valid_POS:
			token_buffer.append(token)
	return token_buffer

def query_papers(session):
	print "Querying titles"
	num_node = session.run("match (n) return count(*) as count").single()['count']
	with io.open(args.title, 'w', encoding = 'utf-16') as f:
		for i in xrange(num_node / epoch + 1):
			lower = i * epoch
			upper = (i + 1) * epoch
			for title in list(session.run("match (p:Paper) where ID(p) >= %d and ID(p) < %d return ID(p) as ID, p.title as title" % (lower, upper))):
				f.write(str(title['ID']) + ', ' + title['title'] + '\n')

		for i in xrange(num_node):
			title_buffer = ""
			for title in list(session.run("match (n)--(p:Paper) where ID(n) = %d and (n:Researcher or n:Conference) return p.title as title" % i)):
				title_buffer += (title['title'] + ' ')
			if title_buffer != "":
				f.write(str(i) + ', ' + title_buffer + '\n')
	f.close()

def convert_to_phrases():
	with io.open(args.title, 'r', encoding = 'utf-16') as fr:
		with io.open(args.phrase, 'w', encoding = 'utf-16') as fw:
			for line in fr:
				line = line[:-1].split(', ', 1)
				phrases = extract_phrases(line[1])
				fw.write(line[0] + ', ' + ' '.join(phrases) + '\n')
		fw.close()
	fr.close()

def learn_vectors():
	print "Learning embeddings"
	document = []
	labels = []
	idx = 0
	with io.open(args.phrase, 'r', encoding = 'utf-16') as f:
		for line in f:
			document.append(LabeledSentence(line.split(', ')[1], [idx]))
			labels.append(line.split(', ')[0])
			idx += 1
	f.close()
	model = Doc2Vec(document, size=64, window=8, min_count=1, workers=4, iter=20)
	with open(args.vector, 'w') as f:
		for i in xrange(len(labels)):
			f.write(str(labels[i]) + ', ' + ' '.join(map(str, model.docvecs[i])) + '\n')
	f.close()

def insert_vectors(session):
	print "Saving vectors to db"
	with open(args.vector, 'r') as f:
		for line in f:
			pair = line[:-1].split(', ', 1)
			session.run("match (n) where ID(n) = %d set n.doc2vec = '%s'" % (int(pair[0]), pair[1]))
	f.close()

def aggregate_vectors(session):
	print "Aggregating doc2vec"
	num_node = session.run("match (n) return count(*) as count").single()['count']
	for i in xrange(num_node):
		paper_vecs = list(session.run("match (n)--(p:Paper) where ID(n) = %d and (n:Researcher or n:Conference) return p.doc2vec as doc2vec" % i))
		if len(paper_vecs) == 0:
			continue
		doc2vec = np.zeros(64)
		for paper_vec in paper_vecs:
			doc2vec += map(float, paper_vec['doc2vec'].split())
		session.run("match (n) where ID(n) = %d set n.doc2vec = '%s'" % (i, ' '.join(map(str, doc2vec))))




driver = GraphDatabase.driver("bolt://localhost", auth = basic_auth("neo4j", "mliu60"))
session = driver.session()

args = parse_args()

query_papers(session)
convert_to_phrases()
# learn_vectors()
# insert_vectors(session)

session.close()


