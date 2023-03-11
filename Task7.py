from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Spark Lab")
sc = SparkContext(conf=conf)
File = sc.textFile("/home/bitnami/Big-Data-Labs/Lab3/Data/kblist")
File1_1 = File.map(lambda line: ((line.split(",")[0]),(line.split(",")[1])))
File1_1_1 = File1_1.collect()
# print(File1_1_1)
# print("==========================================================")

# wordTuples = tokenized.map(lambda word: (word, 1))
# wordTuples = tokenized.map(lambda word: (word, 1))
File2 = sc.textFile("/home/bitnami/Big-Data-Labs/Lab3/Data/weblog.log")
File2_1 = File2.map(lambda line: (line.split(" ")[2],line.split(" ")[6]))
File2_1_filter=File2_1.filter(lambda word:  '.html' in word[1])  
File2_1_filter_aFTER = File2_1_filter.map(lambda line: ((line[1].split('/'))[1].split(".")[0],line[0] ))

# File2_1_1 = File2_1_filter_aFTER.collect()
# print(File2_1_1)

final=File1_1.join(File2_1_filter_aFTER)
final_2=final.map(lambda word:(word[1][1],[word[1][0]]))

wordCounts = final_2.reduceByKey(lambda v1,v2:v1 +v2)

final2 = wordCounts.collect()
for x in final2:
    print(x)
