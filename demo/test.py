from requests import put, get

print get('http://localhost:5000/BasicInfo', params = {'node': 'Researcher', 'name': 'Richard Socher'}).json()
