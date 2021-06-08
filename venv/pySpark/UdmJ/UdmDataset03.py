from pyspark.sql import SparkSession
from pyspark.sql import Row,column as col
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

spark=SparkSession.builder.appName("fromJava01").master("local").getOrCreate()

lst=[("WARN","2016-12-31 04:19:32"),("WARN","2016-12-31 03:21:21"),("FATAL","2016-12-31 03:22:34")
     ,("INFO","2015-04-21 14:32:21"),("FATAL","2015-04-21 19:23:20")]

schema=StructType([
    StructField("level",StringType(),True),
     StructField("date",StringType(),True)
])

df=spark.createDataFrame(lst,schema)
df.show()


df.createOrReplaceTempView("logging")

df=spark.sql("select level,collect_list(date) from logging group by level order by level")
df.show()

df.explain()
