from neo4j.v1 import GraphDatabase, basic_auth
from personalized_pagerank import recommendPaperToResearcher, recommendResearcherToResearcher, recommendResearcherToPaper, recommendPaperToPaper


driver = GraphDatabase.driver("bolt://localhost", auth = basic_auth("neo4j", "mliu60"))
session = driver.session()

def printList(L):
	print "Number of recommendations: %d" % len(L)
	for l in L:
		print l


recommender = recommendPaperToResearcher(session)
print "***Recommend paper to researcher***"
printList(recommender.recommend("Richard Socher", 3))

recommender = recommendResearcherToPaper(session)
print "***Recommend researcher to paper***"
printList(recommender.recommend("Dynamic Memory Networks for Visual and Textual Question Answering.", 3))

recommender = recommendPaperToPaper(session)
print "***Recommend paper to paper***"
printList(recommender.recommend("Dynamic Memory Networks for Visual and Textual Question Answering.", 2))

recommender = recommendResearcherToResearcher(session)
print "***Recommend researcher to researcher***"
printList(recommender.recommend("Richard Socher", 4))	


recommender = recommendPaperToResearcher(session)
print "***Recommend paper to researcher***"
printList(recommender.recommend("Aviva I. Goller", 3))	

recommender = recommendResearcherToPaper(session)
print "***Recommend researcher to paper***"
printList(recommender.recommend("Seeing Sounds and Hearing Colors: An Event-related Potential Study of Auditory-Visual Synesthesia.", 3))	

recommender = recommendPaperToPaper(session)
print "***Recommend paper to paper***"
printList(recommender.recommend("Seeing Sounds and Hearing Colors: An Event-related Potential Study of Auditory-Visual Synesthesia.", 2))	

recommender = recommendResearcherToResearcher(session)
print "***Recommend researcher to researcher***"
printList(recommender.recommend("Aviva I. Goller", 4))	



session.close()
