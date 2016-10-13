from requests import put, get

print get("http://localhost:5000/BasicInfo", params = {"node": "Researcher", "name": "Richard Socher"}).json()
print get("http://localhost:5000/BasicInfo", params = {"node": "Paper", "title": "Dynamic Memory Networks for Visual and Textual Question Answering."}).json()
print get("http://localhost:5000/PublicationHistory", params = {"node": "Researcher", "name": "Kevin Chen-Chuan Chang", "limit": 100}).json()

print get("http://localhost:5000/fullpprRecommend/PtoR", params = {"name": "Richard Socher", "limit": 100}).json()
print get("http://localhost:5000/fullpprRecommend/RtoR", params = {"name": "Richard Socher", "limit": 100}).json()
print get("http://localhost:5000/fullpprRecommend/RtoP", params = {"title": "Dynamic Memory Networks for Visual and Textual Question Answering.", "limit": 100}).json()
print get("http://localhost:5000/fullpprRecommend/PtoP", params = {"title": "Dynamic Memory Networks for Visual and Textual Question Answering.", "limit": 100}).json()
print get("http://localhost:5000/node2vecRecommend/PtoR", params = {"name": "Richard Socher", "limit": 100, "rank_criterion": "Cosine Distance"}).json()
print get("http://localhost:5000/node2vecRecommend/RtoR", params = {"name": "Richard Socher", "limit": 100, "rank_criterion": "Cosine Distance"}).json()
print get("http://localhost:5000/node2vecRecommend/RtoP", params = {"title": "Dynamic Memory Networks for Visual and Textual Question Answering.", "limit": 100, "rank_criterion": "Cosine Distance"}).json()
print get("http://localhost:5000/node2vecRecommend/PtoP", params = {"title": "Dynamic Memory Networks for Visual and Textual Question Answering.", "limit": 100, "rank_criterion": "Cosine Distance"}).json()
print get("http://localhost:5000/fastppvRecommend/PtoR", params = {"name": "Richard Socher", "limit": 100, "rank_criterion": "Cosine Distance"}).json()
print get("http://localhost:5000/fastppvRecommend/RtoR", params = {"name": "Richard Socher", "limit": 100, "rank_criterion": "Cosine Distance"}).json()
print get("http://localhost:5000/fastppvRecommend/RtoP", params = {"title": "Dynamic Memory Networks for Visual and Textual Question Answering.", "limit": 100, "rank_criterion": "Cosine Distance"}).json()
print get("http://localhost:5000/fastppvRecommend/PtoP", params = {"title": "Dynamic Memory Networks for Visual and Textual Question Answering.", "limit": 100, "rank_criterion": "Cosine Distance"}).json()

print get("http://localhost:5000/CompareEmbedding/node2vec", params = {"node1": "Researcher", "node2": "Researcher", "name1": "Jiawei Han", "name2": "ChengXiang Zhai"}).json()
print get("http://localhost:5000/CompareEmbedding/node2vec", params = {"node1": "Researcher", "node2": "Researcher", "name1": "David A. Forsyth", "name2": "Thomas S. Huang"}).json()
