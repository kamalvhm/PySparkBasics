from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
#https://sparkbyexamples.com/pyspark-tutorial/
#initialise spark context  can do { sc=SparkContext() } also
#FIRST WAY TO CREATE Spark Context use "SparkConf() instead of just SparkConf"
conf=SparkConf().setAppName("DEMo").setMaster("local")
sc=SparkContext(conf=conf)
print(sc.getConf().getAll()) # this will return all configuration
sc.stop()

#SECOUND WAY with out any conf
sc=SparkContext()
sc.getConf().getAll()
sc.stop()

#Third WAY
sc =SparkContext("local","First App")


#SparkSession##############################https://www.youtube.com/watch?v=PIa_-aMHYrg&list=PLe1T0uBrDrfMZiiIPqupk-I1Gvn61bLvW
#DataMaking
if __name__=="__main__":
    print("How to create SparkSession ")

spark =SparkSession\
        .builder\
        .appName("How to create Spark Session")\
        .master("local[*]")\
        .enableHiveSupport()\
        .getOrCreate()


print(spark)
print(spark.sparkContext)
print(dir(spark.sparkContext))
print("stopping the sparkSession object")
spark.stop()






