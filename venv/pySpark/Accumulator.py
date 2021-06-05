from pyspark.sql import SparkSession

spark =SparkSession.builder.appName("acc").master("local").getOrCreate()

accum=spark.sparkContext.accumulator(0)
rdd=spark.sparkContext.parallelize([1,2,3,4,5])
rdd.foreach(lambda x:accum.add(x))
print(accum.value)
