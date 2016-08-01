from neo4j.v1 import GraphDatabase, basic_auth
from personalized_pagerank import pprPaperToResearcher, pprResearcherToResearcher, pprResearcherToPaper, pprPaperToPaper


driver = GraphDatabase.driver("bolt://localhost", auth = basic_auth("neo4j", "mliu60"))
session = driver.session()

def printList(L):
	print "Number of recommendations: %d" % len(L)
	for l in L:
		print l


recommender = pprPaperToResearcher(session)
print "***Recommend paper to researcher***"
printList(recommender.recommend("Richard Socher"))

recommender = pprResearcherToPaper(session)
print "***Recommend researcher to paper***"
printList(recommender.recommend("Dynamic Memory Networks for Visual and Textual Question Answering."))

recommender = pprPaperToPaper(session)
print "***Recommend paper to paper***"
printList(recommender.recommend("Dynamic Memory Networks for Visual and Textual Question Answering."))

recommender = pprResearcherToResearcher(session)
print "***Recommend researcher to researcher***"
printList(recommender.recommend("Richard Socher"))	


recommender = pprPaperToResearcher(session)
print "***Recommend paper to researcher***"
printList(recommender.recommend("Aviva I. Goller"))	

recommender = pprResearcherToPaper(session)
print "***Recommend researcher to paper***"
printList(recommender.recommend("Seeing Sounds and Hearing Colors: An Event-related Potential Study of Auditory-Visual Synesthesia."))	

recommender = pprPaperToPaper(session)
print "***Recommend paper to paper***"
printList(recommender.recommend("Seeing Sounds and Hearing Colors: An Event-related Potential Study of Auditory-Visual Synesthesia."))	

recommender = pprResearcherToResearcher(session)
print "***Recommend researcher to researcher***"
printList(recommender.recommend("Aviva I. Goller"))	



session.close()
