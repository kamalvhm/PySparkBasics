from pyspark.sql import SparkSession

spark=SparkSession.builder.appName('Dummy').master("local[4]").getOrCreate()


df=spark.read.csv("D:/PackUp/PySparkBasics/venv/basics/student.csv", header=True,inferSchema=True)

df.repartition(2).write.partitionBy("subject").csv("D:/PackUp/PySparkBasics/venv/basics/output",mode="overwrite")
df.show()

#To read Datafrom Partition
"state=AL and city=SPRINGVILLE"
dfSinglePart = spark.read.option("header", True) \
    .csv("c:/tmp/zipcodes-state/state=AL/city=SPRINGVILLE")
dfSinglePart.printSchema()
dfSinglePart.show()


# This is an example of how to write a Spark DataFrame by preserving the partition columns on DataFrame.

parqDF = spark.read.option("header",True) \
                  .csv("/tmp/zipcodes-state")
parqDF.createOrReplaceTempView("ZIPCODE")
spark.sql("select * from ZIPCODE  where state='AL' and city = 'SPRINGVILLE'") \
    .show()
