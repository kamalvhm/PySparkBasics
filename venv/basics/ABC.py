from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set

spark=SparkSession.builder.appName("null values").master("local").getOrCreate()


lst=[("A","IT",1),("B","IT",2),("C","HR",3),("D","Pay",4)]

columns=["name","dept","count"]

df=spark.createDataFrame(lst,columns)

df.show()

df.select(collect_set("count")).show()



