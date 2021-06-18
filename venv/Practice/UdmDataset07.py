from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format
import pyspark.sql.functions as f

#https://amiradata.com/pyspark-groupby-aggregate-data-in-pyspark/
spark=SparkSession.builder.appName("bigLog").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
#df=spark.read.csv("D:/PackUp/PySparkBasics/venv/Practice/student.csv",header=True,inferSchema=True)
df=spark.read.csv("s3n://vhm22/student.csv",header=True,inferSchema=True)

df=df.groupBy('subject').agg(f.max("score").alias("max_score"),\
                             f.min("score").alias("min_score"))


#df.coalesce(1).write.format("csv").option("header",'true').save("D:/PackUp/PySparkBasics/venv/Practice/results",mode='overwrite')
df.coalesce(1).write.format("csv").option("header",'true').save("s3a://vhm22/results.csv",mode='overwrite')

df.show(10)

# agg() function


