from neo4j.v1 import GraphDatabase, basic_auth
from embedding import node2vecPaperToResearcher, node2vecResearcherToResearcher, node2vecResearcherToPaper, node2vecPaperToPaper, \
					  ppvPaperToResearcher, ppvResearcherToResearcher, ppvResearcherToPaper, ppvPaperToPaper


driver = GraphDatabase.driver("bolt://localhost", auth = basic_auth("neo4j", "mliu60"))
session = driver.session()

def printList(L):
	print "Number of recommendations: %d" % len(L)
	for l in L:
		print l


recommender = node2vecPaperToResearcher(session)
print "***Recommend paper to researcher***"
printList(recommender.recommend("Richard Socher"))

recommender = node2vecResearcherToPaper(session)
print "***Recommend researcher to paper***"
printList(recommender.recommend("Dynamic Memory Networks for Visual and Textual Question Answering."))

recommender = node2vecPaperToPaper(session)
print "***Recommend paper to paper***"
printList(recommender.recommend("Dynamic Memory Networks for Visual and Textual Question Answering."))

recommender = node2vecResearcherToResearcher(session)
print "***Recommend researcher to researcher***"
printList(recommender.recommend("Richard Socher"))


recommender = node2vecPaperToResearcher(session)
print "***Recommend paper to researcher***"
printList(recommender.recommend("Aviva I. Goller"))

recommender = node2vecResearcherToPaper(session)
print "***Recommend researcher to paper***"
printList(recommender.recommend("Seeing Sounds and Hearing Colors: An Event-related Potential Study of Auditory-Visual Synesthesia."))

recommender = node2vecPaperToPaper(session)
print "***Recommend paper to paper***"
printList(recommender.recommend("Seeing Sounds and Hearing Colors: An Event-related Potential Study of Auditory-Visual Synesthesia."))

recommender = node2vecResearcherToResearcher(session)
print "***Recommend researcher to researcher***"
printList(recommender.recommend("Aviva I. Goller"))



session.close()
