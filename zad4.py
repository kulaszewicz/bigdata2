# Wykonane przez Benedykt Kościnski i Jakub Kulaszewicz zad 4

# Shakespeare:
# Total words count: 1022962
# Top 3 Nouns: [(4068, 'no'), (3689, 'are'), (3029, 'king')]
# 10 most frequent words: ['the', 'and', 'i', 'to', 'of', 'a', 'you', 'my', 'that', 'in']

# Mickiewicz:
# Total words count: 581388
# Top 3 Nouns: [(644, 'pan'), (466, 'oczy'), (425, 'ziemi')]
# 10 most frequent words: ['i', 'w', 'siŕ', 'z', 'na', 'nie', 'a', 'do', 'jak', 'to']

# Przyblizony czas wykonania dla Shakespear'a zad2 ~ 2s, natomiast tego ~ 10s

import glob
import io
import os
import re
import shutil
import nltk
import requests
from pyspark import SparkContext
from wiktionaryparser import WiktionaryParser
import time


sc = SparkContext("local", "BigData2 - zad4")

class ShakespeareSettings():
    AUTHOR = 'Shakespeare'
    PATH = "shakespeare/"
    ZIP = "shakespeare.zip"
    ZIP_URL = "https://shakespeare.folger.edu/downloads/txt/shakespeares-works_TXT_FolgerShakespeare.zip"


class MickiewiczSettings():
    AUTHOR = 'Mickiewicz'
    PATH = "mickiewicz/"
    ZIP = "mickiewicz.zip"
    ZIP_URL = "https://download1476.mediafire.com/xedfa7gri72g/zxmc52a0w0ot2mb/6nru63.zip"


def download_file(url, destination):
    """
    This method downloads file from given url and save it to destination path
    Args:
        url: String
        destination: String


    """
    with requests.get(url, stream=True) as r:
        with open(destination, 'wb') as f:
            shutil.copyfileobj(r.raw, f)


def is_noun(word, country_code):
    """
    This method takes word and country_code and returns boolean value based on the word being noun
    Args:
        word: String
        country_code: String

    Returns: Boolean

    """
    if country_code == 'EN':
        for tmp in nltk.corpus.wordnet.synsets(word):
            if tmp.name().split('.')[0] == word and tmp.name().split('.')[1] == 'n':
                return True
            return False
    if country_code == 'PL':
        parser = WiktionaryParser()
        try:
            another_word = parser.fetch(word, 'polish')[0]['definitions'][0]['partOfSpeech']
            if another_word == "noun":
                return True
            else:
                return False
        except:
            return False


def get_author_statistics(Settings, filename, country_code):
    """
    This method gets Settings class, filename and country code and based on given parameters counts and prints to
    console total words count, top 3 nouns and top 10 most frequent words

    Args:
        Settings: Settings<>
        filename: String
        country_code: String

    """

    if country_code == "EN": nltk.download('wordnet')
    download_file(Settings.ZIP_URL, Settings.ZIP)
    shutil.unpack_archive(Settings.ZIP, Settings.PATH)
    files = glob.glob(f'{Settings.PATH}/*.txt')

    start = time.time()
    with io.open(filename, 'a', encoding='utf8') as outfile:
        # Iterate through list
        for names in files:
            # Open each file in read mode
            with io.open(names, 'r', encoding='utf8') as infile:
                # read the data from file1 and
                # file2 and write it in file3
                outfile.write(infile.read())

                # Add '\n' to enter data of file2
            # from next line
            outfile.write("\n")

    logFile = filename

    sc.setLogLevel("ERROR")
    logData = sc.textFile(logFile).cache()

    ileSlowTotal = logData.flatMap(lambda line: line.split(" "))
    ileSlow = logData.filter(lambda tekst: len(tekst) > 0).flatMap(lambda linie: re.split('\W+', linie)).filter(
        lambda slowo: len(slowo) > 0).map(lambda slowo: (slowo.lower(), 1)).reduceByKey(lambda v1, v2: v1 + v2).map(
        lambda x: (x[1], x[0])).sortByKey(ascending=False).persist()
    moja_Lista = ileSlow.take(100)
    popularneRzeczowniki = sc.parallelize(moja_Lista).filter(lambda slowo: is_noun(slowo[1], country_code))

    tylkoSlowa = []
    for i in moja_Lista[:10]:
        tylkoSlowa.append(i[1])

    print(f'{Settings.AUTHOR}:')
    print(f'Total words count: {ileSlowTotal.count()}')
    print(f'Top 3 Nouns: {popularneRzeczowniki.take(3)}')
    print(f'10 most frequent words: {tylkoSlowa}')
    end = time.time()
    print(f'Czas wykonania { end - start }')
    os.remove(filename)


if __name__ == "__main__":
    get_author_statistics(ShakespeareSettings, 'shakespeare.txt', 'EN')
    #get_author_statistics(MickiewiczSettings, 'mickiewicz.txt', 'PL')
