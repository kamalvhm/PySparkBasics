#D:/PackUp/PySparkBasics/venv/pySpark/resources/words.txt

from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("wc").master("local").getOrCreate()

rdd=spark.sparkContext.textFile("D:/PackUp/PySparkBasics/venv/pySpark/resources/words.txt")

rdd=rdd.flatMap(lambda x:x.split(" "))
rdd=rdd.filter(lambda x:len(x)>0)
rdd=rdd.map(lambda x:(x,1))
rdd=rdd.reduceByKey(lambda a,b,:a+b)
rdd=rdd.map(lambda x:(x[1],x[0])).sortByKey(False)


print(rdd.take(10))

rdd.saveAsTextFile("D:/PackUp/PySparkBasics/venv/Practice/result")