from pyspark.sql import SparkSession
from pyspark.sql.functions import * #from pyspark.sql.functions import col,sum,avg,max,udf,lit,approx_count_distinct
from pyspark.sql.window import Window
from pyspark.sql.types import StringType,StructField,StructType,IntegerType
spark = SparkSession.builder.appName('sp.com').getOrCreate()
###########                                                                                                 ####READ##WRITE####  format("com.databrics.spark.xml").option("rowTag","book").load("")
df3 = spark.read.options(header='True', delimiter=',').csv("/tmp/resources/zipcodes.csv",inferSchema=True)
df = spark.read.option("multiline","true").json("resources/multiline-zipcode.json")
df3.write.partitionBy("state").option("header",True).mode("overwrite").csv("/tmp/spark_output/zipcodes123")
df3.write.mode('Overwrite').json("/tmp/spark_output/zipcodes.json")
#######                                                                                                     ####GROUP BY#############
df3.groupBy("department") .agg(sum("salary").alias("sum_salary"),avg("salary").alias("avg_salary"), \
      sum("bonus").alias("sum_bonus"),max("bonus").alias("max_bonus")) \
    .where(col("sum_bonus") >= 50000) .show(truncate=False)
###########                                                                                                 ###FILTER#################
df.filter( (df.state  == "OH") & (df.gender  == "M") ).show(truncate=False)
li=["OH","CA","DE"]
df.filter(df.state.isin(li)).show()
############                                                                                                ####UDF##############
def upperCase(str):
    return str.upper()
upperCaseUDF = udf(lambda z:upperCase(z),StringType())
df.withColumn("Cureated Name", upperCaseUDF(col("Name")))
spark.udf.register("convertUDF", upperCaseUDF,StringType())
#############                                                                                               ###########JOINS
df.join(df3,df.emp_dept_id ==  df3.dept_id,"inner")
for column in [column for column in df3.columns if column not in df.columns]:
    df1 = df1.withColumn(column, lit(None))
#############                                                                                               ###########AGG FUN  AVG,MAX,MIN,MEAN,SUM,SUM DISTINCT,first,last
df.select(approx_count_distinct("salary")).collect()[0][0]
df.select(collect_list("salary")).show(truncate=False)
df.select(countDistinct("department", "salary"))
df.select(sumDistinct("salary")).show(truncate=False)
############### df.dropDuplicates(["department","salary"])   df.distinct()
#df.withColumnRenamed("dob","DateOfBirth").printSchema()
newColumns = ["newCol1","newCol2","newCol3","newCol4"]                                                            #MULTIPLE RENAME
df.toDF(*newColumns).printSchema()
df.na.fill({"city": "unknown", "type": ""})                                                                     #FILL
#############                                                                                               ###########DATE AND TIME
df.select(col("input"),  months_between(current_date(),col("input")).alias("months_between")).show()
df.select(col("input"), datediff(current_date(),col("input")).alias("datediff")).show()
df.select(col("input"),  add_months(col("input"),3).alias("add_months"), date_add(col("input"),4).alias("date_add"),  date_sub(col("input"),4).alias("date_sub")).show()
df.select(col("input"), year(col("input")).alias("year"), month(col("input")).alias("month"), next_day(col("input"),"Sunday").alias("next_day"), weekofyear(col("input")).alias("weekofyear")).show()