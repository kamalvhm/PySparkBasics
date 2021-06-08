from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format
spark=SparkSession.builder.appName("bigLog").master("local").getOrCreate()

df=spark.read.csv("D:/PackUp/PySparkBasics/venv/pySpark/resources/biglog.txt", inferSchema=True).toDF("level","datetime")

#df.show(20)



df=df.select(['level',date_format('datetime',"MMMM").alias("month")
              , date_format('datetime','M').alias('monthNum').cast('integer')])
lst=["January","February","March","April","May","June","july","August","September","October","November","December"]
df=df.groupBy("level").pivot("month",lst).count().na.fill(0)

df.show(50)