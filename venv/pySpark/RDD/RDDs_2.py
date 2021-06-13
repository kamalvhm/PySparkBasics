from pyspark.sql import SparkSession
from pyspark import SparkContext,SparkConf


spark =SparkSession\
     .builder\
     .appName("Rdds")\
    .master("local[*]")\
   .enableHiveSupport()\
 .getOrCreate()

conf=SparkConf().setAppName("DEMo").setMaster("local")
sc=SparkContext(conf=conf)

list=[1,2,3,4,5]
print("printing python list")
print(list)
print(type(list))

print("creating RDD from list")
number_rdd= sc.parallelize(list,3)

print(type(number_rdd))
print(number_rdd.collect())

py_str_list=["Arun","arvind"]
print(py_str_list)
#FIRST WAY TO CREAETE RDD USING COLLECTION
str_rdd=sc.parallelize(py_str_list,2)

print(type(str_rdd))

#SECOUND WAY IS BY ExTERNAL FILE
text_rdd=sc.textFile("resources/boaringwords.txt")


# Creates empty RDD with no partition
rdd = sc.emptyRDD
# rddString = spark.sparkContext.emptyRDD[String]

str_rdd_output=str_rdd.collect()
print("printing output str_rdd")
print(str_rdd_output)

#######CONVERTING RDD TO DF AND VICE VERSA
# Converts RDD to DataFrame
dfFromRDD1 = rdd.toDF()
# Converts RDD to DataFrame with column names
dfFromRDD2 = rdd.toDF("col1","col2")
# using createDataFrame() - Convert DataFrame to RDD
df = spark.createDataFrame(rdd).toDF("col1","col2")
# Convert DataFrame to RDD
rdd = df.rdd

print("stopping the sparkSession object")
sc.stop()