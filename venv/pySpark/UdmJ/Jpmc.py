from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit

spark =SparkSession.builder\
        .appName("jpmc")\
        .master("local")\
        .getOrCreate()

df = spark.read.csv("D:/PackUp/PySparkBasics/venv/pySpark/UdmJ/weather.txt",header=True)
df=df.withColumn("Centi",(col("tempInF").cast("Integer")-32)*5/9)

#df.write.format("parquet").save("D:/PackUp/PySparkBasics/venv/pySpark/UdmJ/output")
#df.write.parquet("/tmp/output/people.parquet")
df.show()