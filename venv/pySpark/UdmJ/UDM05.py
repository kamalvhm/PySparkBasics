from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("und5").master("local").getOrCreate()

rdd=spark.sparkContext.textFile("D:/PackUp/PySparkBasics/venv/pySpark/UdmJ/input.txt")
rdd=rdd.map(lambda stnt:stnt.replace("[^a-zA-Z\\s]", "").lower())
removeBlank=rdd.filter(lambda x:len(x)>0)
justwords=removeBlank.flatMap(lambda x:x.split(" "))
#########INCLUDE BOARING FILTEER
pairRdd=justwords.map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b).map(lambda x:(x[1],x[0]))
sorted=pairRdd.sortByKey(False)
print(sorted.take(10))