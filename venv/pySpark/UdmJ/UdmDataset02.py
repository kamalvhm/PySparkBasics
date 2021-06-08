from pyspark.sql import SparkSession
from pyspark.sql import Row,column as col

spark=SparkSession.builder.appName("fromJava01").master("local").getOrCreate()

df=spark.read.csv("D:/PackUp/PySparkBasics/venv/pySpark/resources/student.csv",header=True,inferSchema=True)

df.show()

firstRow=df.first();
subject=firstRow['subject']
print(subject)
#FIRST
modernArtResults=df.filter("subject ='Modern Art' AND year >=2007")
modernArtResults.show()
#SECOUND
modernArtResults=df.filter((df.subject=='Modern Art') & (df.year>=2007))
modernArtResults.show()
#THIRD
df.createOrReplaceTempView("students")
spark.sql("select avg(score) from students where subject ='English'").show()