from requests import put, get

print get('http://localhost:5000/BasicInfo', params = {'node': 'Researcher', 'name': 'Richard Socher'}).json()
print get('http://localhost:5000/Recommend/PtoR', params = {'name': 'Richard Socher'}).json()
print get('http://localhost:5000/Recommend/RtoR', params = {'name': 'Richard Socher'}).json()
print get('http://localhost:5000/Recommend/RtoP', params = {'title': 'Dynamic Memory Networks for Visual and Textual Question Answering.'}).json()
print get('http://localhost:5000/Recommend/PtoP', params = {'title': 'Dynamic Memory Networks for Visual and Textual Question Answering.'}).json()