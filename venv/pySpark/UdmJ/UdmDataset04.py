from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("bigLog").master("local").getOrCreate()

df=spark.read.csv("D:/PackUp/PySparkBasics/venv/pySpark/resources/biglog.txt", inferSchema=True).toDF("level","datetime")

#df.show(20)

df.createOrReplaceTempView("logging_table")

df=spark.sql("select level,date_format(datetime,'MMMM') as month ,cast(first(date_format(datetime,'M')) as int) monthnum,count(1) as total "
             "from logging_table group by level,month order by monthnum")

df.show(50)