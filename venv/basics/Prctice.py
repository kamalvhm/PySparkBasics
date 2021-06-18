from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark =SparkSession.builder.appName("some").master("local").getOrCreate()


df=spark.read.csv("D:/PackUp/PySparkBasics/venv/basics/student.csv", header=True,inferSchema=True)
df.show()

df.withColumn("RESULT",expr("CASE  WHEN score > 45 THEN 'Pass' ELSE 'Fail' END")).show()