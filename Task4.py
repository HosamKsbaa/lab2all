from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Spark Lab")
sc = SparkContext(conf=conf)
File = sc.textFile("Big-Data-Labs/Lab3/Data/AliceInWonderLandPart1.txt")
tokenized = File.flatMap(lambda line: line.split(" "))
wordTuples =tokenized.distinct()
wordCounts = wordTuples.map(lambda word: (word, len(word)))
aaa = wordCounts.map(lambda word: word[1])

zz=aaa.max()
print("the max num is "+ str(zz))
final=wordCounts.filter(lambda word: word[1] == zz )

list_words = final.collect()
print(list_words)