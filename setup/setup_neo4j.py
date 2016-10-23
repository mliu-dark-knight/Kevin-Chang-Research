import csv
import os
from neo4j.v1 import GraphDatabase, basic_auth


driver = GraphDatabase.driver("bolt://localhost:7687", auth = basic_auth("neo4j", "mliu60"))
session = driver.session()


datafile = "CoAuthor.csv"

def match(line):
    if str(line):
        line= line.strip().replace("'", "").split(",")
        for i in range(len(line)):
            for j in range(i+1,len(line)):
                session.run("MATCH (a:Author1 {name:'"+line[i]+"'}),(b:Author1 {name: '"+line[j]+"'}) MERGE (a)-[r:coauthor]->(b) ON CREATE SET r.number  = 1 ON MATCH SET r.number = r.number +1")

with open(datafile,'rb') as f:
    author = f.readline()
    for line in f:
        for author in line.strip().replace("'", "").split(","):
            if str(author):
                session.run("MERGE (n:Author1 {name:'"+str(author)+"'}) ON CREATE SET n.number  = 1 ON MATCH SET n.number = n.number +1")
        match(line)
        
f.close()


session.close()
