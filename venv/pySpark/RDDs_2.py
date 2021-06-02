from pyspark.sql import SparkSession
from pyspark import SparkContext,SparkConf


#spark =SparkSession\
#      .builder\
#     .appName("Rdds")\
#     .master("local[*]")\
#   .enableHiveSupport()\
#  .getOrCreate()
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

str_rdd=sc.parallelize(py_str_list,2)
print(type(str_rdd))

str_rdd_output=str_rdd.collect()
print("printing output str_rdd")
print(str_rdd_output)

print("stopping the sparkSession object")
sc.stop()