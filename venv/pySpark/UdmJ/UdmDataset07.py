from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format
import pyspark.sql.functions as f

#https://amiradata.com/pyspark-groupby-aggregate-data-in-pyspark/
spark=SparkSession.builder.appName("bigLog").master("local").getOrCreate()

df=spark.read.csv("D:/PackUp/PySparkBasics/venv/pySpark/resources/student.csv",header=True,inferSchema=True)


df.show(50)

# agg() function


