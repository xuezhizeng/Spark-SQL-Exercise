from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import string

sc = SparkContext("local", "myapp")
# Create SQL Context
sq = SQLContext(sc)

wordRDD = sc.textFile("/home/yanan/PycharmProjects/data_mng_as3/rowWordCountData.txt")

def stripContent(myline):
    translator = str.maketrans('', '', string.punctuation)
    myline = myline.lower().translate(translator)
    return myline

wordRDDtokens = wordRDD.flatMap(lambda lines: stripContent(lines).split()).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
with open("output", 'w') as file:
    file.write("Total words: " + str(len(wordRDDtokens.collect())) + '\n')
    for pair in wordRDDtokens.collect():
        file.write("{}\t\t{}\n".format(pair[0], pair[1]))
