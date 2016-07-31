from requests import put, get

print get("http://localhost:5000/BasicInfo", params = {"node": "Researcher", "name": "Richard Socher"}).json()
print get("http://localhost:5000/BasicInfo", params = {"node": "Paper", "title": "Dynamic Memory Networks for Visual and Textual Question Answering."}).json()
print get("http://localhost:5000/Recommend/PtoR", params = {"name": "Richard Socher"}).json()
print get("http://localhost:5000/Recommend/RtoR", params = {"name": "Richard Socher"}).json()
print get("http://localhost:5000/Recommend/RtoP", params = {"title": "Dynamic Memory Networks for Visual and Textual Question Answering."}).json()
print get("http://localhost:5000/Recommend/PtoP", params = {"title": "Dynamic Memory Networks for Visual and Textual Question Answering."}).json()

print get("http://localhost:5000/BasicInfo", params = {"node": "Researcher", "name": "Aviva I. Goller"}).json()
print get("http://localhost:5000/BasicInfo", params = {"node": "Paper", "title": "Seeing Sounds and Hearing Colors: An Event-related Potential Study of Auditory-Visual Synesthesia."}).json()
print get("http://localhost:5000/Recommend/PtoR", params = {"name": "Aviva I. Goller"}).json()
print get("http://localhost:5000/Recommend/RtoR", params = {"name": "Aviva I. Goller"}).json()
print get("http://localhost:5000/Recommend/RtoP", params = {"title": "Seeing Sounds and Hearing Colors: An Event-related Potential Study of Auditory-Visual Synesthesia."}).json()
print get("http://localhost:5000/Recommend/PtoP", params = {"title": "Seeing Sounds and Hearing Colors: An Event-related Potential Study of Auditory-Visual Synesthesia."}).json()

print get("http://localhost:5000/CompareEmbedding", params = {"node": "Researcher", "name1": "Jiawei Han", "name2": "ChengXiang Zhai"}).json()
print get("http://localhost:5000/CompareEmbedding", params = {"node": "Researcher", "name1": "David A. Forsyth", "name2": "Thomas S. Huang"}).json()
print get("http://localhost:5000/CompareEmbedding", params = {"node": "Researcher", "name1": "Nikita Borisov", "name2": "Michael Bailey"}).json()
print get("http://localhost:5000/CompareEmbedding", params = {"node": "Researcher", "name1": "Jiawei Han", "name2": "Thomas S. Huang"}).json()
print get("http://localhost:5000/CompareEmbedding", params = {"node": "Researcher", "name1": "Nikita Borisov", "name2": "Chengxiang Zhai"}).json()

