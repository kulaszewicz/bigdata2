# Wykonane przez Benedykt Kościnski i Jakub Kulaszewicz zad 5 bez covida

# Shakespeare:
# Total words count: 1022962
# Top 3 Nouns: [(4068, 'no'), (3689, 'are'), (3029, 'king')]
# 10 most frequent words: ['the', 'and', 'i', 'to', 'of', 'a', 'you', 'my', 'that', 'in']
# Word categories: [('Tiny', 38395), ('Small', 594668), ('Medium', 359438), ('Big', 30461)]
# Language used: English
# Time to calculate ~18s

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

sc = SparkContext("local", "BigData2 - zad5")


class ShakespeareSettings():
    AUTHOR = 'Shakespeare'
    PATH = "shakespeare/"
    ZIP = "shakespeare.zip"
    ZIP_URL = "https://shakespeare.folger.edu/downloads/txt/shakespeares-works_TXT_FolgerShakespeare.zip"


class MickiewiczSettings():
    AUTHOR = 'Mickiewicz'
    PATH = "mickiewicz/"
    ZIP = "mickiewicz.zip"
    ZIP_URL = "https://download1476.mediafire.com/k5cq6wxapsfg/zxmc52a0w0ot2mb/6nru63.zip"


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


def get_word_category(word):
    """
    Get word category based on it's length
    Args:
        word: <string>

    Returns:
        category: <string>
    """
    word_l = len(word)
    if word_l == 1:
        return 'Tiny'
    if word_l <= 4:
        return 'Small'
    if word_l <= 9:
        return 'Medium'
    if word_l >= 10:
        return 'Big'


def get_words_categories(words):
    """
    Categorizes given words from Tiny to Big
    Args:
        words: <SparkContext>

    Returns:
        categories: <SparkContext>

    """
    return words.map(lambda word: (get_word_category(word), 1)).reduceByKey(lambda x, y: x + y) \
        .sortByKey(ascending=False).collect()


def get_all_letters(text):
    """
    Gives all letters in given text
    Args:
        text: <SparkContext>

    Returns:
        letters: <SparkContext>

    """
    return text.map(lambda slowo: list(slowo)).flatMap(lambda l: l)


def get_letters_occurrence_count(letters):
    """
    Gives letters occurrences count
    Args:
        letters: <SparkContext>

    Returns:
        letters_occurrences_count: <SparkContext>
    """
    return letters.map(lambda letter: (letter.lower(), 1)).reduceByKey(lambda x, y: x + y).map(lambda x: (x[1], x[0])) \
        .sortByKey(ascending=False)


def define_language_used(letters_with_occurrence_count):
    """
    Defines text language based on letter analysis
    Args:
        letters_with_occurrence_count:
            <SparkContext>[...[letterOccurrences, letter]]
    Returns:
        'Polish' || 'English' <String>
    """
    pl_PL = "iaeozn"
    en_EN = "etaoin"
    top_6_letters = letters_with_occurrence_count.take(6)
    pl_PL_count = sc.parallelize(top_6_letters).map(lambda l: l[1]).filter(lambda l: pl_PL.count(l)).collect()
    en_EN_count = sc.parallelize(top_6_letters).map(lambda l: l[1]).filter(lambda l: en_EN.count(l)).collect()
    if len(pl_PL_count) > len(en_EN_count):
        return 'Polish'
    else:
        return "English"


def get_author_statistics(Settings, filename, country_code):
    """
    This method gets Settings class, filename and country code and based on given parameters counts and prints to
    console total words count, top 3 nouns and top 10 most frequent words, words categories and language used

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

    words_total = logData.flatMap(lambda line: line.split(" "))
    letters_total = words_total.map(lambda slowo: list(slowo)).flatMap(lambda l: l)
    letters_occurrences = get_letters_occurrence_count(letters_total)
    language_used = define_language_used(letters_occurrences)
    words_count = logData.filter(lambda tekst: len(tekst) > 0).flatMap(lambda linie: re.split('\W+', linie)).filter(
        lambda slowo: len(slowo) > 0).map(lambda slowo: (slowo.lower(), 1)).reduceByKey(lambda v1, v2: v1 + v2).map(
        lambda x: (x[1], x[0])).sortByKey(ascending=False).persist()
    words_categories = get_words_categories(words_total)
    top_100_words = words_count.take(100)
    popular_nouns = sc.parallelize(top_100_words).filter(lambda slowo: is_noun(slowo[1], country_code))

    only_words = []
    for i in top_100_words[:10]:
        only_words.append(i[1])

    print(f'{Settings.AUTHOR}:')
    print(f'Total words count: {words_total.count()}')
    print(f'Top 3 Nouns: {popular_nouns.take(3)}')
    print(f'10 most frequent words: {only_words}')
    print(f'Word categories: {words_categories}')
    print(f'Language used: {language_used}')
    end = time.time()
    print(f'Time to calculate {end - start}')
    os.remove(filename)


if __name__ == "__main__":
    get_author_statistics(ShakespeareSettings, 'shakespeare.txt', 'EN')
    get_author_statistics(MickiewiczSettings, 'mickiewicz.txt', 'PL')
