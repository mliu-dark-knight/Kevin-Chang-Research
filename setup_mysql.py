import MySQLdb

# Open database connection
db = MySQLdb.connect("engr-cpanel2.engr.illinois.edu", "mliu60_mliu60", "mliu60", "mliu60_summer2016" )

# prepare a cursor object using cursor() method
cursor = db.cursor()


cursor.execute("CREATE TABLE Researcher (name VARCHAR(128) NOT NULL, pgrank DOUBLE, PRIMARY KEY (name))")
cursor.execute("CREATE TABLE Paper (title VARCHAR(512) NOT NULL, year INT, platform VARCHAR(256), type VARCHAR(16), pgrank DOUBLE, PRIMARY KEY (title))")
cursor.execute("CREATE TABLE AuthorOf (name VARCHAR(128) NOT NULL, title VARCHAR(512), PRIMARY KEY (name, title), " + 
			   "CONSTRAINT FOREIGN KEY (name) REFERENCES Researcher(name), CONSTRAINT FOREIGN KEY (title) REFERENCES Paper(title))")
cursor.execute("CREATE TABLE CoAuthor (name1 VARCHAR(128) NOT NULL, name2 VARCHAR(128) NOT NULL, count INT, PRIMARY KEY (name1, name2), " + 
			   "CONSTRAINT FOREIGN KEY (name1) REFERENCES Researcher(name), CONSTRAINT FOREIGN KEY (name2) REFERENCES Researcher(name))")


def strip_quotation(input):
	return input.replace('"', '').replace("'", "")


with open("Paper.csv") as f:
	next(f)
	count = 1
	for line in f:
		data = line[:-1].split(',')
		try:
			assert len(data) == 4
		except:
			print "Error: Parse error %d" % count
			count += 1
		else:
			title = strip_quotation(data[0])
			year = strip_quotation(data[1])
			platform = strip_quotation(data[2])
			ptype = strip_quotation(data[3])
			query = "INSERT INTO Paper(title, year, platform, type, pgrank) VALUES('%s', '%s', '%s', '%s', 0.0)" % (title, year, platform, ptype)
			try:
				cursor.execute(query)
			except:
				print "Error: unable to execute query"
db.commit()



with open("Researcher.csv") as f:
	next(f)
	count = 1
	for line in f:
		name = line[:-1]
		query = "INSERT INTO Researcher(name, pgrank) VALUES('%s', 0.0)" % (name)
		try:
			cursor.execute(query)
		except:
			print "Error: unable to execute query %d" % count
			count += 1
db.commit()


with open("AuthorOf.csv") as f:
	next(f)
	count = 1
	for line in f:
		data = line[:-1].split(',')
		try:
			assert len(data) == 2
		except:
			print "Error: Parse error %d" % count
			count += 1
		else:
			name = strip_quotation(data[0])
			title = strip_quotation(data[1])
			query = "INSERT INTO AuthorOf(name, title) VALUES('%s', '%s')" % (name, title)
			try:
				cursor.execute(query)
			except:
				print "Error: unable to execute query"
db.commit()


# execute SQL query using execute() method.
cursor.execute("SELECT DISTINCT(name) from Researcher, Paper where Researcher.title = Paper.title")

# Fetch a single row using fetchone() method.
names = cursor.fetchall()

for name in names:
	try:
		query = "INSERT INTO Researcher(name, pgrank) VALUES('%s', 0.0)" % (name)
	except:
		print "Error: unable to execute query"



with open("CoAuthor.csv") as f:
	next(f)
	for line in f:
		names = line[:-1].split(',')

		if len(names != 1):
			for i in range(len(names)):
				for j in range(i+1, len(names)):
					query = "SELECT FROM CoAuthor WHERE name1 = '%s' AND name2 = '%s'" % (min(names[i], names[j]), max(names[i], names[j]))
					try:
						cursor.execute(query)
					except:
						print "Error: unable to execute query"
					else:
						count = cursor.fetchall()
						assert len(count) <= 1
						if (len(count) == 0):
							query = "INSERT INTO CoAuthor(name1, name2, count) VALUES('%s', '%s', 1)" % (min(names[i], names[j]), max(names[i], names[j]))
						else:
							query = "UPDATE CoAuthor SET count = count + 1 WHERE name1 = '%s' AND name2 = '%s'" % (min(names[i], names[j]), max(names[i], names[j]))
						try:
							cursor.execute(query)
						except:
							print "Error: unable to execute query"
db.commit()

# disconnect from server
db.close()

