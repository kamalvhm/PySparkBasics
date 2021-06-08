from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format
spark=SparkSession.builder.appName("bigLog").master("local").getOrCreate()

df=spark.read.csv("D:/PackUp/PySparkBasics/venv/pySpark/resources/biglog.txt", inferSchema=True).toDF("level","datetime")

#df.show(20)



df=df.select(['level',date_format('datetime',"MMMM").alias("month")
              , date_format('datetime','M').alias('monthNum').cast('integer')])

df=df.groupBy("level","monthNum").count()
df=df.orderBy("level","monthNum")

df.show(50)