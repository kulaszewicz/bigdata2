#!/usr/bin/env python
# coding: utf-8

# In[75]:


# Wykonane przez Jakub Kulaszewicz s17838 Benedykt Kosciński s17472
# Liczba wyrazow = 154598

import requests
from bs4 import BeautifulSoup
import pandas as pd


# In[76]:


url = "https://wolnelektury.pl/katalog/autor/adam-mickiewicz/"


# In[77]:


r = requests.get(url)


# In[78]:


soup = BeautifulSoup(r.content)


# In[79]:


print(soup.prettify())


# In[80]:


mick = soup.find("div", {"class": "plain-list"})
totalWordCount = 0
for link in mick.find_all('a'):
    title = str(link.get('href')).split('/')[-2]
    goToLink = f'https://wolnelektury.pl/media/book/txt/{title}.txt'
    newRequest = requests.get(goToLink)
    if (newRequest.status_code == 200):
        bookSoap = BeautifulSoup(newRequest.content)
        textToCount = bookSoap.prettify().split('-----')[0]
        textToCount = textToCount.replace('—', '')
        totalWordCount += len(textToCount.split())
        #print(title)
        #print(len(textToCount.split()))
        
#Brakujace z zrodla powyzej

# + Dziady
for czescDziadow in range(5):
    dziadyRequest = requests.get(f'https://literat.ug.edu.pl/dziady/00{czescDziadow+1}.htm');
    dziadySoap = BeautifulSoup(dziadyRequest.content)
    text = dziadySoap.find('blockquote').getText()
    totalWordCount += len(text.split())
    
# + Ballady i romansy

for czescBallad in range(12):
    balladaRequest = requests.get(f'https://literat.ug.edu.pl/amwiersz/{czescBallad+7:04}.htm')
    balladaSoap = BeautifulSoup(balladaRequest.content)
    text = balladaSoap.find('blockquote').getText()
    totalWordCount += len(text.split())

# + Sonety Krymskie

for sonetKrymski in range(16):
    sonetRequest = requests.get(f'https://literat.ug.edu.pl/amwiersz/{sonetKrymski+31:04}.htm')
    sonetSoap = BeautifulSoup(sonetRequest.content)
    text = sonetSoap.find('blockquote').getText()
    totalWordCount += len(text.split())
    
# + Sonety

for sonet in range(6):
    sonetRequest = requests.get(f'https://literat.ug.edu.pl/amwiersz/{sonet+26:04}.htm')
    sonetSoap = BeautifulSoup(sonetRequest.content)
    text = sonetSoap.find('blockquote').getText()
    totalWordCount += len(text.split())

# # + Liryki lozanskie

for liryka in range(12):
    lirykaRequest = requests.get(f'https://literat.ug.edu.pl/amwiersz/{liryka+74:04}.htm')
    lirykaSoap = BeautifulSoup(lirykaRequest.content)
    text = lirykaSoap.find('blockquote').getText()
    totalWordCount += len(text.split())
    
print(f'Liczba wszystkich wyrazow napisanych przez Adama Mickiewicza = {totalWordCount}')
    


# In[ ]:





# In[ ]:




