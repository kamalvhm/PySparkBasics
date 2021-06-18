from pyspark.sql import SparkSession
import pyspark.sql.functions as f

spark =SparkSession.builder.appName("grp").master("local").getOrCreate()
'''https://sparkbyexamples.com/pyspark/pyspark-groupby-explained-with-example/
count() - Returns the count of rows for each group.
mean() - Returns the mean of values for each group.
max() - Returns the maximum of values for each group.
min() - Returns the minimum of values for each group.
sum() - Returns the total for values for each group.
avg() - Returns the average for values for each group
agg() - Using agg() function, we can calculate more than one aggregate at a time.
pivot() - This function is used to Pivot the DataFrame which 
'''
simpleData = [("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  ]

schema = ["employee_name","department","state","salary","age","bonus"]
df=spark.createDataFrame(simpleData,schema)
df.show()

print(" groupBy() on department column of DataFrame and then find the sum of salary for each department using sum() aggregate function.")
#df1=df.groupBy(df["department"]).sum("salary")
# df1.show()
print("Similarly, we can calculate the number of employee in each department using count()")
#df.groupBy("department").count().show()
print("Calculate the minimum salary of each department using min()")
#data_counts=df.groupBy("department").min("salary")
#data_counts.show()
#One way to get all columns after doing a groupBy is to use join function joib "by department"
#df.join(data_counts, "department").dropDuplicates()
print("Calculate the maximin salary of each department using max()")
#df.groupBy("department").max("salary").show()
print("Calculate the average salary of each department using avg()")
#df.groupBy("department").avg("salary").show()
print("Calculate the mean salary of each department using mean()")
#df.groupBy("department").mean("salary").show()
print("groupBy and aggregate on multiple columns group by on department,state and does sum() on salary and bonus columns.")
#df.groupBy("department","state").sum("salary","bonus").show()
print("Using agg() aggregate function we can calculate many aggregations at a time on a single statement")
#using PySpark SQL aggregate functions sum(), avg(), min(), max() mean() e.t.c. In order to use these,
#we should import """from pyspark.sql.functions import sum,avg,max,min,mean,count"""
    # df.groupBy("department") \
    #     .agg(f.sum("salary").alias("sum_salary"), \
    #          f.avg("salary").alias("avg_salary"), \
    #          f.sum("bonus").alias("sum_bonus"), \
    #          f.max("bonus").alias("max_bonus") \
    #      ) \
    #     .show(truncate=False)
print("Similar to SQL “HAVING” clause, On PySpark DataFrame we can use either where() or filter() function to filter the rows of aggregated data")
df.groupBy("department") \
    .agg(f.sum("salary").alias("sum_salary"), \
      f.avg("salary").alias("avg_salary"), \
      f.sum("bonus").alias("sum_bonus"), \
      f.max("bonus").alias("max_bonus")) \
    .where(f.col("sum_bonus") >= 50000) \
    .show(truncate=False)