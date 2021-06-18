from pyspark.sql import SparkSession
from pyspark.sql import col,lit
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]
columns = ["firstname","middlename","lastname","dob","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)

"""1. Change DataType using PySpark withColumn()"""
#By using PySpark withColumn() on a DataFrame, we can cast or change the data type of a column.
df.withColumn("salary",col("salary").cast("Integer")).show()

"""2. Update The Value of an Existing Column"""
#PySpark withColumn() function of DataFrame can also be used to change the value of an existing column
df.withColumn("salary",col("salary")*100).show()

"""3.Create a Column from an Existing"""
#To add/create a new column, specify the first argument with a name you want your new column to be
df.withColumn("CopiedColumn",col("salary")* -1).show()

"""4. Add a New Column using withColumn()"""
df.withColumn("Country", lit("USA")).show()
df.withColumn("Country", lit("USA")) \
  .withColumn("anotherColumn",lit("anotherValue")) \
  .show()

"""5. Rename Column Name"""
df.withColumnRenamed("gender","sex") \
  .show(truncate=False)

"""6. Drop Column From PySpark DataFrame"""
df.drop("salary").show()

