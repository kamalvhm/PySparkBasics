from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("wc").master("local").getOrCreate()

lst=["WARN: Tuesday 4 September 0405",
     "ERROR: Tuesday 4 September 0408",
     "FATAL: Wednesday 5 September 1632",
     "ERROR: Friday 7 September 1854","WARN: Saturday 8 September 1942"]

rdd =spark.sparkContext.parallelize(lst)
rdd=rdd.flatMap(lambda x:x.split(":"))
rdd=rdd.filter(lambda x:len(x)<6).map(lambda x:(x,1))
rdd=rdd.reduceByKey(lambda a,b:a+b)
rdd.foreach(lambda x:print(x))