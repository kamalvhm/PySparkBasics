from pyspark.sql import SparkSession


spark=SparkSession.builder.appName("").master("local").getOrCreate()


df=spark.read.csv("D:/PackUp/PySparkBasics/venv/basics/employee.csv",header=True,inferSchema=True)

#df.show()
df.createOrReplaceTempView("Student")

df =spark.sql("select name ,gender ,salary ,dense_rank() over (order by salary desc) as ranks from student ")

df.show()

print("selecting third highest salary ")

df.select("name","salary","gender").where("ranks=3").show()