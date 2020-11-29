# !/usr/bin/env python
# coding: utf-8

# Wykonane przez Jakub Kulaszewicz s17838 Benedykt Kosciński s17472
# 10 najpopularniejszych słów i 3 najczęściej występujących rzeczowników w dziełach Shakespeare'a.
# Wynik:
# 10 most common words: the, and, to, of, i, a, my, in, you, is
# 3 most common nouns: i, a, the

from collections import Counter
import collections
import requests
import shutil
import nltk
import glob


class Settings():
    SHAKESPEARE_PATH = "shakespeare/"
    SHAKESPEARE_ZIP = "shakespeare.zip"
    SHAKESPEARE_ZIP_URL = "https://shakespeare.folger.edu/downloads/txt/shakespeares-works_TXT_FolgerShakespeare.zip"


def download_file(url, destination):
    with requests.get(url, stream=True) as r:
        with open(destination, 'wb') as f:
            shutil.copyfileobj(r.raw, f)


def unpack_archive(target_path, unpacking_path):
    shutil.unpack_archive(target_path, unpacking_path)


def count_word_occurrences(string):
    data = string.lower().split()
    return collections.Counter(data)


def prepare_artwork(file_name):
    with open(file_name, 'r') as artwork:
        data = ''.join(artwork.read().split('\n')[8:])
    return data


def get_nouns(words):
    nltk.download('wordnet')
    nouns = dict()
    for w in words:
        for tmp in nltk.corpus.wordnet.synsets(w):
            if tmp.name().split('.')[0] == w and tmp.name().split('.')[1] == 'n':
                nouns[w] = tmp.pos()
    return nouns


download_file(Settings.SHAKESPEARE_ZIP_URL, Settings.SHAKESPEARE_ZIP)
shutil.unpack_archive(Settings.SHAKESPEARE_ZIP, Settings.SHAKESPEARE_PATH)

files = glob.glob(f'{Settings.SHAKESPEARE_PATH}/*.txt')

words_score = Counter()
for file in files:
    words_score += count_word_occurrences(prepare_artwork(file))

words = list(map(lambda word: word[0], words_score.most_common(200)))
nouns = get_nouns(words)

print("10 most common words:", *words_score.most_common(10))
print("3 most common nouns:", list(nouns.keys())[:3])
