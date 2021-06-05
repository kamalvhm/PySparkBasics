from pyspark.sql import SparkSession


spark=SparkSession.builder\
    .appName("wc")\
    .master("local[*]")\
    .getOrCreate()

rdd=spark.sparkContext.textFile("D:\PackUp\PySparkBasics\venv\pySpark\resources\words.txt");
rdd.count();