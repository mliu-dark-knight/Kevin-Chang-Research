import json
import io
import argparse
from pycorenlp import StanfordCoreNLP
from neo4j.v1 import GraphDatabase, basic_auth

epoch = 50000
nlp = StanfordCoreNLP('http://localhost:9000')
valid_POS = set(['NN', 'NNP', 'NNS', 'JJ'])

def parse_args():
	parser = argparse.ArgumentParser()
	parser.add_argument('--title', nargs='?', default='title.txt')
	parser.add_argument('--phrase', nargs='?', default='phrase.txt')
	return parser.parse_args()


def extract_phrases(text) :
	phrases = []
	tokens = nlp.annotate(text, properties = {'annotators': 'pos', 'outputFormat': 'json'})['sentences'][0]['tokens']

	for token in tokens:
		if token['pos'] in valid_POS:
			phrases.append(token['word'])
	return phrases

def query_papers(session):
	num_node = session.run("match (n) return count(*) as count").single()['count']
	with io.open(args.title, 'w', encoding = 'utf-16') as f:
		for i in xrange(num_node / epoch + 1):
			lower = i * epoch
			upper = (i+1) * epoch
			for title in list(session.run("Match (p:Paper) where ID(p) >= %d and ID(p) < %d return ID(p) as ID, p.title as title" % (lower, upper))):
				f.write(str(title['ID']) + ', ' + title['title'] + '\n')
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


driver = GraphDatabase.driver("bolt://localhost", auth = basic_auth("neo4j", "mliu60"))
session = driver.session()

args = parse_args()
query_papers(session)
convert_to_phrases()

session.close()


