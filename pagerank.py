from neo4j.v1 import GraphDatabase, basic_auth
import numpy as np
from scipy.sparse import csc_matrix
from scipy.sparse.linalg import spsolve


# driver = GraphDatabase.driver("bolt://localhost:7687", auth = basic_auth("neo4j", "mliu60"))
# session = driver.session()

initilize = """
			match (node) set node.pagerank = 0.0
			"""

iterate = """
		  match (node)
		  with collect(distinct node) as pages
		  unwind pages as dest
		  match (source)-[:AuthorOf]-(dest)
		  with collect(distinct source) as sources, dest as dest
		  unwind sources as src
		  match (src)-[r:AuthorOf]-()
		  with src.pagerank / count(r) as points, dest as dest
		  with sum(points) as p, dest as dest
		  set dest.pagerank = 0.15 * dest.pagerank + 0.85 * p;
		  """



row = np.random.randint(5033098, size = 25185978)
col = np.random.randint(5033098, size = 25185978)
data = np.random.randint(5033098, size = 25185978)
matrix = csc_matrix((data, (row, col)), shape=(5033098, 5033098))

initial = np.full((5033098, 1), 1.0 / 5033098.0)

result = matrix.dot(initial)

print result

# result = spsolve(matrix, np.random.randint(5033098, size = 5033098))

# session.close()