from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Spark Lab")
sc = SparkContext(conf=conf)
File = sc.textFile("Big-Data-Labs/Lab3/Data/AliceInWonderLandPart1.txt")
tokenized = File.flatMap(lambda line: line.split(" "))
wordTuples = tokenized.map(lambda word: (word, 1))
wordCounts = wordTuples.reduceByKey(lambda v1,v2:v1 +v2)
list_words = wordCounts.collect()
print(list_words)