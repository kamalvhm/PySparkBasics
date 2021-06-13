from pyspark.sql import SparkSession
from pyspark import SparkContext,SparkConf


#spark =SparkSession\
#      .builder\
#     .appName("Rdds")\
#     .master("local[*]")\
#   .enableHiveSupport()\
#  .getOrCreate()
def upper(input):
    output=None
    output=input.upper()
    return output

conf=SparkConf().setAppName("DEMo").setMaster("local")
sc=SparkContext(conf=conf)

list=[1,2,3,4,5]
print("printing python list")
print(list)
print(type(list))
# Converting Each name in uSquare
print("creating RDD from list")
number_rdd= sc.parallelize(list,3)
number_rdd=number_rdd.map(lambda n:n*n)
print(number_rdd.collect())
# Converting Each name in upper case
py_str_list=["Arun","arvind","Govind"]
print(py_str_list)
str_rdd=sc.parallelize(py_str_list,2)
str_rdd=str_rdd.map(upper)
print(str_rdd.collect())



text_rdd=sc.textFile("resources/boaringwords.txt")
start_with_t_rdd=text_rdd.filter(lambda ele: 't' in ele) #words only contains t
t_list=start_with_t_rdd.collect()
print(t_list)

start_with_t_rdd=start_with_t_rdd.map(lambda n:n.lower())
print(start_with_t_rdd.collect())

print("********MAP PARTITION*******")
print(number_rdd.getNumPartitions())


print("stopping the sparkSession object")
sc.stop()