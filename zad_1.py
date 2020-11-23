#!/usr/bin/env python
# coding: utf-8

# In[11]:


# Wykonane przez Jakub Kulaszewicz s17838 Benedykt Kosciński s17472
# Liczba wyrazow = 154598

import requests
from bs4 import BeautifulSoup
import pandas as pd


# In[12]:


url = "https://wolnelektury.pl/katalog/autor/adam-mickiewicz/"


# In[13]:


r = requests.get(url)


# In[14]:


soup = BeautifulSoup(r.content)


# In[8]:


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

def addMissingMaterials(link):
    global totalWordCount;
    request = requests.get(link);
    requestSoaped = BeautifulSoup(request.content);
    text = requestSoaped.find('blockquote').getText();
    totalWordCount += len(text.split());
    
# + Dziady
for czescDziadow in range(5):
    link = f'https://literat.ug.edu.pl/dziady/00{czescDziadow+1}.htm';
    addMissingMaterials(link);
    
# + Ballady i romansy

for czescBallad in range(12):
    link = f'https://literat.ug.edu.pl/amwiersz/{czescBallad+7:04}.htm';
    addMissingMaterials(link);

# + Sonety Krymskie

for sonetKrymski in range(16):
    link = f'https://literat.ug.edu.pl/amwiersz/{sonetKrymski+31:04}.htm';
    addMissingMaterials(link);
    
# + Sonety

for sonet in range(6):
    link = f'https://literat.ug.edu.pl/amwiersz/{sonet+26:04}.htm';
    addMissingMaterials(link);

# # + Liryki lozanskie

for liryka in range(12):
    link = f'https://literat.ug.edu.pl/amwiersz/{liryka+74:04}.htm';
    addMissingMaterials(link);
    
print(f'Liczba wszystkich wyrazow napisanych przez Adama Mickiewicza = {totalWordCount}')
    


# In[ ]:





# In[ ]:




