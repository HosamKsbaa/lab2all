from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Spark Lab")
sc = SparkContext(conf=conf)
File = sc.textFile("/home/bitnami/Big-Data-Labs/Lab3/Task6/ml-100k/u.txt")
# tokenized = File.flatMap(lambda line: line.split(" "))
tuple_pairs = File.map(lambda line: (int(line.split("\t")[1]),int(line.split("\t")[2])))
wordCounts_rating = tuple_pairs.reduceByKey(lambda v1,v2:v1 +v2)

tuple_pairs2 = File.map(lambda line: (int(line.split("\t")[1]),1))
wordCounts2_count = tuple_pairs2.reduceByKey(lambda v1,v2:v1 +v2)
z=wordCounts_rating+wordCounts2_count
wordCounts2_Rating_over_Count = z.reduceByKey(lambda v1,v2:v1 /v2)

# list_words = wordCounts_rating.collect()
# list_words2 = wordCounts2_count.collect()
list_words3 =wordCounts2_Rating_over_Count.collect()
# print(list_words)
# print("+++++++++++++++++")
# print(list_words2)
# print("+++++++++++++++++")

print(list_words3)