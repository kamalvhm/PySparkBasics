from pyspark.sql import SparkSession
##https://amiradata.com/pyspark-groupby-aggregate-data-in-pyspark/
spark =SparkSession.builder.appName("aggs").master("local").getOrCreate()

df=spark.read.csv('D:/PackUp/PySparkBasics/venv/UDM/resources/sales_info.csv',inferSchema=True,header=True)
df.show()
df.printSchema()

df.groupBy("Company")
#gives Each Company average sales
df.groupBy("Company").mean().show()
#df.groupBy("Company").sum().show()
#df.groupBy("Company").max().show()
#df.groupBy("Company").count().show()
#This returns sum of all the Sales in DF it takes dictionary as input {Column :function}
df.agg({'Sales':'sum'}).show()
#we can parform agg on group data
group_data=df.groupBy("Company")
group_data.agg({'Sales':'max'}).show()

#we can import function
from pyspark.sql.functions import countDistinct

#in spark 2.1
#from pyspark.sql.functions import avg,stddev
df.select(countDistinct('Sales').alias("COUNTs")).show()

#df.agg({"Sales":"stddev"}).show()
from pyspark.sql.functions import format_number
#sales_std=df.select(stddev('Sales').alias("std")).show()
#sales_std.select()format_number('std',2).show()
df.orderBy("Sales").show() #ASC
df.orderBy(df['Sales'].desc()).show() #For Desc

