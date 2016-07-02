import pyorient
import pandas as pd

Researcher = ["name", "idx", "pgrank"]
Paper = ["title", "year", "platform", "type"]
AuthorOf = ["name", "title"]

client = pyorient.OrientDB("localhost", 2424)  # host, port

### open a connection (username and password)
client.connect("root", "WCup20140613")

### select to use that database
client.db_open("summer2016", "root", "WCup20140613")


client.command("create class Researcher extends V")
client.command("create property Researcher.name String")
client.command("create property Researcher.idx Integer")
client.command("create property Researcher.pgrank Double")

client.command("create class Paper extends V")
for attribute in Paper:
	query = "create property Paper.%s String" % (attribute)
	client.command(query)

client.command("create class AuthorOf")
for attribute in AuthorOf:
	query = "create property AuthorOf.%s String" % (attribute)
	client.command(query)

client.command("create class Writes extends E")



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
			query = "insert into Paper(title, year, platform, type) values('%s', '%s', '%s', '%s')" % (title, year, platform, ptype)
			try:
				client.command(query)
			except:
				print "Error: unable to execute query" 


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
			query = "insert into AuthorOf(name, title) values('%s', '%s')" % (name, title)
			try:
				client.command(query)
			except:
				print "Error: unable to execute query"


idx = 0
names = []

def insert_callback(result):
	global names
	names.append(result.distinct)


for char in ['&', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']:
	names = []
	print char

	result = client.query_async("select distinct(name) from AuthorOf where name like '%s'" % (char + "%"), 1000000, '*:0', insert_callback)

	for name in names:
		query = "insert into Researcher(name, idx, pgrank) values('%s', '%d', '%f')" % (name, idx, 0.0)
		try:
			client.command(query)
		except:
			print "Error: unable to execute query"
		else:
			idx += 1


all_researchers = []
reseachers = []

def append_callback(result):
	global reseachers
	reseachers.append(result.name)
	

for char in ['&', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']:
	reseachers = []
	print char

	result = client.query_async("select name from Researcher where name like '%s'" % (char + "%"), 1000000, '*:0', append_callback)

	all_researchers += reseachers




df = pd.DataFrame(all_researchers, columns=["name"])
df.to_csv("Researcher.csv", index=False, encoding='utf-8')


client.db_close()

