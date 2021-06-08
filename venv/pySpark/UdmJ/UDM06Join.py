from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,ShortType,IntegerType

spark=SparkSession.builder.appName("udm").master("local").getOrCreate()

left=[(1,2,3,4),(9,10,11,12),(5,6,8,13)]
right=[(1,2,3,4),(5,6,7,8)]
print(left)
print(right)

leftRdd=spark.sparkContext.parallelize(left)
rightRdd=spark.sparkContext.parallelize(right)

leftPairs=leftRdd.map(lambda x:((x[0],x[1],x[2]),x[3]))
rightPairs=rightRdd.map(lambda x:((x[0],x[1],x[2]),x[3]))

print("Left RDD ",leftRdd.count())
print("Right RDD ",rightRdd.count())

joined=leftPairs.join(rightPairs)

print("Joined ",joined.count())

print(type(joined))

joined.foreach(lambda x:print(x))