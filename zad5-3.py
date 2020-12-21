# Wykonane przez Benedykt Kościnski i Jakub Kulaszewicz zad 5.

# Analiza przypadków zachorowani na COVID19. Liczba zachorowani na milion populacji.
# Wyniki:
# Najwięcej największa liczba zachorowani na milion populacji: USA - 27997

import glob
import io
import os
import re
import shutil
import nltk
import requests
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import pyspark.sql.functions as func

import time

sc = SparkContext('local', 'BigData2 - zad4')
spark = SparkSession.builder.appName('BigData2 - zad4').getOrCreate()

def min(data):
    return data.takeOrdered(1, key=lambda x: x[1])

def max(data):
    return data.takeOrdered(1, key=lambda x: -x[1])

def main():
    URL = 'https://corona-api.com/countries'
    data_raw = requests.get(URL).json()['data']
    data = sc.parallelize(data_raw)\
        .filter(lambda country: country['population'] is not None)\
        .map(lambda country: (country['name'], country['latest_data']['calculated']['cases_per_million_population']))

    max = max(data)



    df = data.toDF(['country', 'cases/mil'])


    print(data.collect())




if __name__ == "__main__":
    main()
    #get_author_statistics(MickiewiczSettings, 'mickiewicz.txt', 'PL')