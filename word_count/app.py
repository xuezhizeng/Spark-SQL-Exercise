from pyspark import SparkContext
import string
import time

start = time.time()
sc = SparkContext("local", "myapp")

wordRDD = sc.textFile("rowWordCountData.txt")

def stripContent(myline):
    translator = str.maketrans('', '', string.punctuation)
    myline = myline.lower().translate(translator)
    return myline

wordRDDtokens = wordRDD.flatMap(lambda lines: stripContent(lines).split())\
    .map(lambda word: (word, 1))\
    .reduceByKey(lambda a, b: a+b)

with open("output", 'w') as file:
    file.write("Total words: " + str(len(wordRDDtokens.collect())) + '\n')
    for pair in wordRDDtokens.collect():
        file.write("{}\t\t{}\n".format(pair[0], pair[1]))

end = time.time()
elapsed = end - start
print("elapsed time: " + str(elapsed))
