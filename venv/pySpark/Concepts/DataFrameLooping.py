from pyspark.sql import SparkSession

"""1)PySpark provides map(), mapPartitions() to loop/iterate through rows in RDD/DataFrame to perform the complex transformations,
 and these two returns the same number of records as in the original DataFrame but the number of columns could be different
 
2)PySpark also provides foreach() & foreachPartitions() actions to loop/iterate through each Row (DOES NOT RETURN ANYTHING)
 
3)instead of iterating through using map() and foreach(), 
you should use either DataFrame select() or DataFrame withColumn() in conjunction with PySpark SQL functions.
 
"""

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = [('James','Smith','M',30),('Anna','Rose','F',41),
  ('Robert','Williams','M',62),
]
columns = ["firstname","lastname","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
#df.show()

"""3 way to iterate using select and with column"""
from pyspark.sql.functions import concat_ws,col,lit
df.select(concat_ws(",",df.firstname,df.lastname).alias("name"), \
          df.gender,lit(df.salary*2).alias("new_salary")).show()

"""1) Using map() to Loop Through Rows in DataFrame
PySpark doesn’t have a map() in DataFrame instead it’s in RDD hence we need to convert DataFrame to RDD first and then use the map()
If you have a heavy initialization use PySpark mapPartitions() transformation instead of map()
"""
# Refering columns by index.
rdd=df.rdd.map(lambda x:
    (x[0]+","+x[1],x[2],x[3]*2)
    )
df2=rdd.toDF(["name","gender","new_salary"])
df2.show()

# By Calling function
def func1(x):
    firstName=x.firstname
    lastName=x.lastName
    name=firstName+","+lastName
    gender=x.gender.lower()
    salary=x.salary*2
    return (name,gender,salary)

rdd2=df.rdd.map(lambda x: func1(x))

"""2) Using foreach() to Loop Through Rows in DataFrame
Similar to map(), foreach() also applied to every row of DataFrame, the difference being foreach() is an action and it returns nothing.
"""
# Foreach example
def f(x): print(x)
df.foreach(f)

# Another example
df.foreach(lambda x:
    print("Data ==>"+x["firstname"]+","+x["lastname"]+","+x["gender"]+","+str(x["salary"]*2))
    )

"""USING Collect Data As List and Loop Through"""

# Collect the data to Python List
dataCollect = df.collect()
for row in dataCollect:
    print(row['firstname'] + "," +row['lastname'])

#Using toLocalIterator()
dataCollect=df.rdd.toLocalIterator()
for row in dataCollect:
    print(row['firstname'] + "," +row['lastname'])

