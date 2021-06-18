from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("04").master("local").getOrCreate()

lst=["WARN: Tuesday 4 September 0405",
     "ERROR: Tuesday 4 September 0408",
     "FATAL: Wednesday 5 September 1632",
     "ERROR: Friday 7 September 1854","WARN: Saturday 8 September 1942"]

sc=spark.sparkContext

stance=spark.sparkContext.parallelize(lst)

words=stance.flatMap(lambda x:x.split(" "))

filtered=words.filter(lambda x:len(x)>4)

filtered.foreach(lambda x:print(x))