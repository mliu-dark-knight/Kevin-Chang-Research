from neo4j.v1 import GraphDatabase, basic_auth


driver = GraphDatabase.driver("bolt://localhost:7687", auth = basic_auth("neo4j", "mliu60"))
session = driver.session()

initilize =
	"""
	match (node) set node.pagerank = 0.0
	"""

iterate =
	"""
	match (node)
	with collect(distinct node) as pages
	unwind pages as dest
		match (source)-[]-(dest)
		with collect(distinct source) as sources, dest as dest
		unwind sources as src
			match (src)-[r:]->()
			with src.pageRank / count(r) as points, dest as dest
			with sum(points) as p, dest as dest
			set dest.pageRank = 0.15 + 0.85 * p;
	"""

# session.run(initilize)
for i in range(1):
	session.run(iterate)

session.close()