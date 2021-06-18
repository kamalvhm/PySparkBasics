from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

#Create DataFrame df1 with columns name,dept & age
data = [("James","Sales",34), ("Michael","Sales",56), \
    ("Robert","Sales",30), ("Maria","Finance",24) ]
columns= ["name","dept","age"]
df1 = spark.createDataFrame(data = data, schema = columns)
#df1.printSchema()
df1.show()

#Create DataFrame df1 with columns name,dep,state & salary
data2=[("James","Sales","NY",9000),("Maria","Finance","CA",9000), \
    ("Jen","Finance","NY",7900),("Jeff","Marketing","CA",8000)]
columns2= ["name","dept","state","salary"]
df2 = spark.createDataFrame(data = data2, schema = columns2)
#df2.printSchema()
df2.show()
"""
In Spark or PySpark let’s see how to merge/unione two DataFrames with a different number of columns (different schema).
In Spark 3.1, you can easily achieve this using unionByName()
transformation by passing allowMissingColumns with the value true. In order version, this property is not available

merged_df = df1.unionByName(df2, allowMissingColumns=True)

The difference between unionByName() function and union() is that this function
resolves columns by name (not by position). In other words, unionByName() is used to merge two DataFrame’s 
by column names instead of by position.
"""

###Now add missing columns ‘state‘ and ‘salary‘ to df1 and ‘age‘ to df2 with null values.
#Add missing columns 'state' & 'salary' to df1
from pyspark.sql.functions import lit
for column in [column for column in df2.columns if column not in df1.columns]:
    df1 = df1.withColumn(column, lit(None))

#Add missing column 'age' to df2
for column in [column for column in df1.columns if column not in df2.columns]:
    df2 = df2.withColumn(column, lit(None))


#Finally join two dataframe's df1 & df2 by name
merged_df=df1.unionByName(df2)
merged_df.show()



