
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import *
from pyspark.sql import SparkSession, Row

columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
rdd = spark.sparkContext.parallelize(data)

#1)Create DataFrame from RDD |https://sparkbyexamples.com/pyspark/different-ways-to-create-dataframe-in-pyspark/

################################1.1 Using toDF() function
dfFromRDD1 = rdd.toDF()
dfFromRDD1.printSchema()
#If you wanted to provide column names to the DataFrame use toDF()
# method with column names as arguments as shown below.
columns = ["language","users_count"]
dfFromRDD1 = rdd.toDF(columns)
dfFromRDD1.printSchema()
###############################1.2 Using createDataFrame() from SparkSession
#Using createDataFrame() from SparkSession is another way to create manually
# and it takes rdd object as an argument. and chain with toDF() to specify
# name to the columns.
dfFromRDD2 = spark.createDataFrame(rdd).toDF(*columns)

#2) Create DataFrame from List Collection
dfFromData2 = spark.createDataFrame(data).toDF(*columns)
#2.2 Using createDataFrame() with the Row type
rowData = map(lambda x: Row(*x), data)
dfFromData3 = spark.createDataFrame(rowData,columns)
#2.3 Create DataFrame with schema
data2 = [("James", "", "Smith", "36636", "M", 3000),
         ("Michael", "Rose", "", "40288", "M", 4000),
         ("Robert", "", "Williams", "42114", "M", 4000),
         ("Maria", "Anne", "Jones", "39192", "F", 4000),
         ("Jen", "Mary", "Brown", "", "F", -1)
         ]

schema = StructType([ \
    StructField("firstname", StringType(), True), \
    StructField("middlename", StringType(), True), \
    StructField("lastname", StringType(), True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True)
    ])

df = spark.createDataFrame(data=data2, schema=schema)
df.printSchema()
df.show(truncate=False)

#3. Create DataFrame from Data sources
#3.1 Creating DataFrame from CSV
df2 = spark.read.csv("/src/resources/file.csv")
#3.2. Creating from text (TXT) file
df2 = spark.read.text("/src/resources/file.txt")