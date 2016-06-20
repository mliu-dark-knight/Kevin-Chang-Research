import MySQLdb
import pandas as pd

# Open database connection
db = MySQLdb.connect(host = "engr-cpanel2.engr.illinois.edu", user = "mliu60_mliu60", passwd = "mliu60", db = "mliu60_summer2016" )

# prepare a cursor object using cursor() method
cursor = db.cursor()

rows = []

# Drop table if it already exist using execute() method.
try:
	cursor.execute("SELECT title from Paper")
	rows = cursor.fetchall()

except:
	print "Error: unable to execute query"

idx = 0
dictionary = {}
wdlist = []
for row in rows:
	words = row[0].split()
	for word in words:
		if word not in dictionary:
			dictionary[word] = idx
			idx += 1

df = pd.DataFrame(dictionary.items(), columns=["word", "idx"])
df.to_csv("vocab.csv", index=False)
# disconnect from server
db.close()
