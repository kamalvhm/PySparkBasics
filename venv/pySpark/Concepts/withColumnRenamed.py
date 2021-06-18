import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import *
from pyspark.sql.functions import col

"""Use PySpark withColumnRenamed() to rename a DataFrame column, we often need to rename one column or multiple (or all)
 columns on PySpark DataFrame, you can do this in several ways. When columns are nested it becomes complicated."""

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

dataDF = [(('James', '', 'Smith'), '1991-04-01', 'M', 3000),
          (('Michael', 'Rose', ''), '2000-05-19', 'M', 4000),
          (('Robert', '', 'Williams'), '1978-09-05', 'M', 4000),
          (('Maria', 'Anne', 'Jones'), '1967-12-01', 'F', 4000),
          (('Jen', 'Mary', 'Brown'), '1980-02-17', 'F', -1)
          ]

schema = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('lastname', StringType(), True)
    ])),
    StructField('dob', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('salary', IntegerType(), True)
])

df = spark.createDataFrame(data=dataDF, schema=schema)
df.printSchema()
""" Note that withColumnRenamed function returns a new DataFrame and doesn’t modify the current DataFrame."""
# Example 1
df.withColumnRenamed("dob", "DateOfBirth").printSchema()

"""2. PySpark withColumnRenamed – To rename multiple columns"""
df2 = df.withColumnRenamed("dob", "DateOfBirth") \
    .withColumnRenamed("salary", "salary_amount")
df2.printSchema()

"""3. Using PySpark StructType – To rename a nested column in Dataframe"""
"""Changing a column name on nested data is not straight forward and we can do this by creating a new schema 
with new DataFrame columns using StructType and use it using cast function as shown below."""
schema2 = StructType([
    StructField("fname", StringType()),
    StructField("middlename", StringType()),
    StructField("lname", StringType())])

df.select(col("name").cast(schema2),
          col("dob"),
          col("gender"),
          col("salary")) \
    .printSchema()

"""4. Using Select – To rename nested elements."""
df.select(col("name.firstname").alias("fname"),
          col("name.middlename").alias("mname"),
          col("name.lastname").alias("lname"),
          col("dob"), col("gender"), col("salary")) \
    .printSchema()

"""5. Using PySpark DataFrame withColumn – To rename nested columns"""
#When you have nested columns on PySpark DatFrame and if you want to rename it, use withColumn on a data frame object to
# create a new column from an existing and we will need to drop the existing column
df4 = df.withColumn("fname", col("name.firstname")) \
    .withColumn("mname", col("name.middlename")) \
    .withColumn("lname", col("name.lastname")) \
    .drop("name")
df4.printSchema()

"""7. Using toDF() – To change all columns in a PySpark DataFrame"""
#When we have data in a flat structure (without nested) , use toDF() with a new schema to change all column names.
newColumns = ["newCol1", "newCol2", "newCol3", "newCol4"]
df.toDF(*newColumns).printSchema()

# Example 6
'''
not working
old_columns = Seq("dob","gender","salary","fname","mname","lname")
new_columns = Seq("DateOfBirth","Sex","salary","firstName","middleName","lastName")
columnsList = old_columns.zip(new_columns).map(f=>{col(f._1).as(f._2)})
df5 = df4.select(columnsList:_*)
df5.printSchema()
'''
