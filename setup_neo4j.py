import csv
import os
import time
from neo4j.v1 import GraphDatabase, basic_auth


driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "WCup20140613"))
session = driver.session()


session.run("CREATE CONSTRAINT ON (n:Researcher) ASSERT n.name is UNIQUE")
session.run("CREATE CONSTRAINT ON (n:Paper) ASSERT n.title is UNIQUE")


def strip(line):
	return line.strip().replace('"', '').replace("'", "")



def match_coauthor(line):
	if str(line):
		line = strip(line).split(",")
		for i in range(len(line)):
			for j in range(i+1,len(line)):
				try:
					session.run("MATCH (a:Researcher {name:'"+line[i]+"'}),(b:Researcher {name: '"+line[j]+"'}) MERGE (a)-[r:CoAuthor]-(b) ON CREATE SET r.count  = 1 ON MATCH SET r.count = r.count + 1")
				except:
					pass
		


epoch = 0
start = time.time()
with open("CoAuthor.csv", 'rb') as f:
	author = f.readline()
	for line in f:
		for author in strip(line).split(","):
			if str(author):
				try:
					session.run("MERGE (n:Researcher {name:'%s'})" % str(author))
				except:
					pass
		match_coauthor(line)

		if epoch % 1000 == 0:
			print "Epoch: %d" % (epoch / 1000)
			print "Time: %ds" % (time.time() - start)
		epoch += 1
f.close()
print "Finish inserting Researchers & CoAuthors"
print "****************"



epoch = 0
start = time.time()
with open("Paper.csv", 'rb') as f:
	count = 0
	author = f.readline()
	for line in f:
		line = strip(line).split(",")
		try:
			assert len(line) == 3
		except:
			print "Error: Parse error %d" % count
			count += 1
		else:
			title = line[0]
			year = line[1]
			platform = line[2]
			if not str(title):
				continue
			try:
				session.run("MERGE (n:Paper {title:'%s', year:%d, platform:'%s'})" % (title, int(year), platform))
			except:
				print "Error: unable to execute query %d" % count
				count += 1
			

		if epoch % 1000 == 0:
			print "Epoch: %d" % (epoch / 1000)
			print "Time: %ds" % (time.time() - start)
		epoch += 1
f.close()
print "Finish inserting Papers"
print "****************"



epoch = 0
start = time.time()
with open("AuthorOf.csv", 'rb') as f:
	count = 0
	author = f.readline()
	for line in f:
		line = strip(line).split(",")
		try:
			assert len(line) == 2
		except:
			print "Error: Parse error %d" % count
			count += 1
		else:
			name = line[0]
			title = line[1]
			try:
				session.run("MATCH (a:Researcher {name:'%s'}),(b:Paper {name: '%s'}) MERGE (a)-[r:AuthorOf]-(b))" % (name, title))
			except:
				print "Error: unable to execute query %d" % count
				count += 1
			

		if epoch % 1000 == 0:
			print "Epoch: %d" % (epoch / 1000)
			print "Time: %ds" % (time.time() - start)
		epoch += 1
f.close()
print "Finish inserting AuthorOf"
print "****************"



session.close()
