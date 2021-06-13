from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("wc").master("local").getOrCreate()


sc=spark.sparkContext
rdd=sc.textFile("D:/PackUp/PySparkBasics/venv/pyspark/resources/bigLog2.txt")

rdd=rdd.flatMap(lambda x:x.split(" "))
#converted to all lover
rdd=rdd.map(lambda x:x.lower())

stopwords = ['is','am','are','the','for','a']
#filtering stop words
rdd=rdd.filter(lambda x:x not in stopwords)
#print(rdd.take(10),end="")
#grouping by first three chars  **********IMP**********
rdd4=rdd.groupBy(lambda x:x[0:3])
# tuple=rdd4.take(1)
# print (tuple[0][0])
# it=iter(tuple[0][1])
# for elem in it:
#     print(elem,end=" ")
#print(rdd.take(20))
rdd_mapped=rdd.map(lambda x:(x,1))

frequencyRdd=rdd_mapped.reduceByKey(lambda a,b:a+b).map(lambda x:(x[1],x[0])).sortByKey(False)

#print(frequencyRdd.take(10))
""" https://www.analyticsvidhya.com/blog/2016/10/using-pyspark-to-perform-transformations-and-actions-on-rdd/
##Q5: How do I perform a task (say count the words ‘spark’ and ‘apache’ in rdd3)
# separatly on each partition and get the output of the task performed in these partition ?
Soltion: We can do this by applying “mapPartitions” transformation. The “mapPartitions” is like a map transformation but 
runs separately on different partitions of a RDD. So, 
for counting the frequencies of words ‘spark’ and ‘apache’ in each partition of RDD, you can follow the steps:
1)Create a function called “func” which will count the frequencies for these words
2)Then, pass the function defined in step1 to the “mapPartitions” transformation.

I have used the “glom” function which is very useful when we want to see the data insights for each partition of a RDD. 
"""

def func(iterator):
    countSpark=0
    countApache=0
    for i in iterator:
        if i=='spark':
            countSpark+=1
        if i=='apache':
            countApache+=1
    return countApache,countSpark
rdd.repartition(2)
print(rdd.mapPartitions(func).glom().collect())