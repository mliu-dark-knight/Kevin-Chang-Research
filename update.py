import sys
import urllib2
import xml.etree.cElementTree as ET
from xml.etree.ElementTree import iterparse, XMLParser
from bs4 import BeautifulSoup
from neo4j.v1 import GraphDatabase, basic_auth


month = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06', 
		 'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}

reload(sys)
sys.setdefaultencoding('utf-8')



'''
True only when pubdate is later than curdate
'''
def compare(curdate, pubdate):
	curdate = curdate.split(' ')
	cday = int(curdate[0])
	cmonth = int(month[curdate[1]])
	cyear = int(curdate[2])

	pubdate = pubdate.split(' ')
	tday = int(pubdate[0])
	tmonth = int(month[pubdate[1]])
	tyear = int(pubdate[2])

	if tyear > cyear:
		return True
	if tyear < cyear:
		return False
	if tmonth > cmonth:
		return True
	if tmonth < cmonth:
		return False
	if tday > cday:
		return True
	return False


def stripComma(input):
	return input.strip().replace(', ', '').replace(',', ' ')


def parseLink(link, session, skipFirst):
	html = BeautifulSoup(urllib2.urlopen(link))
	div = html.find('div', {'id': 'breadcrumbs', 'class': 'section'})
	platform = stripComma(div.findAll('span', {'itemprop': 'title'})[2].text)

	try:
		platformID = session.run("merge (p:Platform {platform:'%s'}) return ID(p) as ID" % platform).single()['ID']
	except:
		print "Error: cannot execute query"
		return

	print "Platform: %s" % platform

	uls = html.findAll('ul', {'class': 'publ-list'})
	if skipFirst:
		entry = stripComma(uls[0].find('span', {'class': 'title', 'itemprop': 'name'}).text)

	for ul in uls:
		names = ul.findAll('span', {'itemprop': 'name'})
		authorIDs = []
		for name in names:
			if name.has_attr('class') and name['class'][0] == 'title':
				paper = stripComma(name.text)
				print "Paper: %s" % paper

				if skipFirst and paper == entry:
					authorIDs = []
					continue

				try:
					paperID = session.run("create (p:Paper{title:'%s'}) return ID(p) as ID" % paper).single()['ID']
				except:
					print "Error: cannot execute query"
					authorIDs = []
					continue

				session.run("match (pp:Paper), (pt:Platform) where ID(pp) = %d and ID(pt) = %d create (pp)-[:PublishAt]->(pt)" % (paperID, platformID))
				for authorID in authorIDs:
					session.run("match (r:Researcher), (p:Paper) where ID(r) = %d and ID(p) = %d create (r)-[:AuthorOf]->(p)" % (authorID, paperID))
				authorIDs = []

			else:
				author = stripComma(name.text)
				print "Researcher: %s" % author
				try:
					authorIDs.append(session.run("merge (r:Researcher {name:'%s'}) return ID(r) as ID" % author).single()['ID'])
				except:
					print "Error: cannot execute query"

		assert authorIDs == []


def parseAll(url, session, skipFirst):
	f = open('log.txt', 'rw')
	curdate =f.readline()
	html = BeautifulSoup(urllib2.urlopen(url), "lxml")
	newdate = html.find('item').find('pubdate').text[5:16]
	f.write(newdate)
	f.close()

	for item in html.findAll('item'):
		pubdate = item.find('pubdate').text[5:16]
		guid = item.find('guid').text

		if not compare(curdate, pubdate):
			break
		parseLink(guid, session, skipFirst)




driver = GraphDatabase.driver("bolt://localhost:7687", auth = basic_auth("neo4j", "mliu60"))
session = driver.session()

parseAll("http://dblp.uni-trier.de/news/conf-update.rss", session, True)
parseAll("http://dblp.uni-trier.de/news/jour-update.rss", session, False)

session.close()


