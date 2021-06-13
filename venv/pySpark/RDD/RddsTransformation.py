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
number_rdd=number_rdd.filter(lambda n:n%2==0)
print(number_rdd.collect())

input_file_path=""
text_rdd=sc.textFile("resources/boaringwords.txt")
start_with_t_rdd=text_rdd.filter(lambda ele: 't' in ele) #words only contains t
t_list=start_with_t_rdd.collect()
print(t_list)


print("stopping the sparkSession object")
sc.stop()