
# coding: utf-8

# In[7]:

import xml.etree.cElementTree as ET
from xml.etree.ElementTree import iterparse, XMLParser
import htmlentitydefs
import csv
import sys
import re


reload(sys)
sys.setdefaultencoding('utf-8')

# In[8]:

class CustomEntity:
    def __getitem__(self, key):
        if key == 'umml':
            key = 'uuml' # Fix invalid entity
        return unichr(htmlentitydefs.name2codepoint[key])


# In[9]:

article_file = "dblp.xml"


# In[10]:

parser = XMLParser()
parser.parser.UseForeignDTD(True)
parser.entity = CustomEntity()


# In[11]:

outfile = "CoAuthor.csv"


# In[12]:

with open(outfile, "w") as f:
    w = csv.writer(f)
    w.writerow(["names"])
    for event, elem in iterparse('dblp.xml', events=['start'], parser=parser):
        if elem.tag in set(['article', 'incollection', 'inproceedings']):
            a = []
            for author in elem.findall('author'):
                if str(author.text):
                    a.append(str(author.text))
            w.writerow(a)
        elem.clear()


# In[13]:

f.close()


# In[ ]:



