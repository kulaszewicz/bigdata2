#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
from bs4 import BeautifulSoup
import pandas as pd


# In[8]:


url = "https://wolnelektury.pl/katalog/autor/adam-mickiewicz/"


# In[9]:


r = requests.get(url)


# In[10]:


soup = BeautifulSoup(r.content)


# In[11]:


print(soup.prettify())


# In[35]:


mick = soup.find("div", {"class": "plain-list"})
totalWordCount = 0
for link in mick.find_all('a'):
    title = str(link.get('href')).split('/')[-2]
    goToLink = f'https://wolnelektury.pl/media/book/txt/{title}.txt'
    newRequest = requests.get(goToLink)
    if (newRequest.status_code == 200):
        bookSoap = BeautifulSoup(newRequest.content)
        #print(bookSoap.prettify())
        totalWordCount += len(bookSoap.prettify().split())
        #print(title)
        #print(len(bookSoap.prettify().split()))
print(f'Liczba wszystkich wyrazow: {totalWordCount}')
    


# In[ ]:





# In[ ]:




