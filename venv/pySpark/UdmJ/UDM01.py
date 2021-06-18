from pyspark.sql import SparkSession
import math
spark=SparkSession.builder.appName("01").master("local").getOrCreate()
sc=spark.sparkContext
lst=[35.5,12,49,90.5,20.5]
sc.setLogLevel("ERROR")

rdd=sc.parallelize(lst)
#REDUCE
result=rdd.reduce(lambda v1,v2:v1+v2)
print("REDUCE RESULT",result)

#MAP
lst2=[1,2,3,4]
rdd2=sc.parallelize(lst2)

sqrtRdd=rdd2.map(lambda x:math.sqrt(x))
sqrtRdd.foreach(lambda x:print(x))
print(sqrtRdd.count())
