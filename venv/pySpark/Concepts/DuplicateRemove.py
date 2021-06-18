import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
'''
Duplicate rows could be remove or drop from Spark SQL DataFrame using distinct() and dropDuplicates() functions, 
distinct() can be used to remove rows that have the same values on all columns whereas dropDuplicates() can be used
to remove rows that have the same values on multiple selected columns.
'''
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = [("James", "Sales", 3000), \
    ("Michael", "Sales", 4600), \
    ("Robert", "Sales", 4100), \
    ("Maria", "Finance", 3000), \
    ("James", "Sales", 3000), \
    ("Scott", "Finance", 3300), \
    ("Jen", "Finance", 3900), \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000), \
    ("Saif", "Sales", 4100) \
  ]
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = data, schema = columns)
df.printSchema()
df.show(truncate=False)
#Use distinct() – Remove Duplicate Rows on DataFrame
#On the above dataset, we have a total of 10 rows and one row with all values duplicated,
# performing distinct on this DataFrame should get us 9 as we have one duplicate.
"""FIRST WAY DELETE ALL DUPLICATES """
print("After Distinct removal")
distinctDf=df.distinct()
distinctDf.show()

#Alternatively, you can also run dropDuplicates() function which returns a new DataFrame after removing duplicate rows.
#Spark doesn’t have a distinct method that takes columns that should run distinct on however, Spark provides another signature
# of dropDuplicates() function which takes multiple columns to eliminate duplicates.
"""SECOUND WAY DELETE ALL DUPLICATES """
print("After drop duplicates")
df2 = df.dropDuplicates()
print("Distinct count: "+str(df2.count()))
df2.show(truncate=False)

#you can pass column to look for duplicates
dropDisDF = df.dropDuplicates(["department","salary"])
print("Distinct count of department & salary : "+str(dropDisDF.count()))
dropDisDF.show(truncate=False)
