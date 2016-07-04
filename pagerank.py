from neo4j.v1 import GraphDatabase, basic_auth


driver = GraphDatabase.driver("bolt://localhost:7687", auth = basic_auth("neo4j", "mliu60"))
session = driver.session()

initilize =
	"""
	match (a) set a.pagerank = 0.0
	"""

iterate =
	"""
	match (a)
	with collect(distinct a) as nodes,count(a) as num_nodes
	unwind nodes as a
	match (a)-[r]-(b)
	with a,collect(r) as rels, count(r) as num_rels,1.0/num_nodes as rank
		unwind rels as rel
	   	set endnode(rel).pagerank =
			case
				when num_rels > 0 and id(startnode(rel)) = id(a) then
					endnode(rel).pagerank + rank/(num_rels)
				else endnode(rel).pagerank
			end
		,
		startnode(rel).pagerank =
			case
				when num_rels > 0 and id(endnode(rel)) = id(a) then
					startnode(rel).pagerank + rank/(num_rels)
				else startnode(rel).pagerank
			end
	"""

# session.run(initilize)
for i in range(1):
	session.run(iterate)

session.close()