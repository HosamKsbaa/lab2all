from pyspark import SparkContext, SparkConf
import sys
countThreshold = int(sys.argv[1])
print(countThreshold)
conf = SparkConf().setAppName("Spark Lab")
sc = SparkContext(conf=conf)
File = sc.textFile("Big-Data-Labs/Lab3/Data/AliceInWonderLandPart1.txt")
tokenized = File.flatMap(lambda line: line.split(" "))
wordTuples = tokenized.map(lambda word: (word, 1))
wordCounts = wordTuples.reduceByKey(lambda v1,v2:v1 +v2)

wordCounts2=wordCounts.filter(lambda word: word[1] < countThreshold  )

list_words = wordCounts2.collect()

print(list_words)