from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf,col

spark =SparkSession.builder.appName("udf").master("local").getOrCreate()

'''
UDF’s are used to extend the functions of the framework and re-use these functions on multiple DataFrame’s.
For example, you wanted to convert every first letter of a word in a name string to a capital case; PySpark
build-in features don’t have this function hence you can create it a UDF and reuse this as needed on many 
Data Frames. UDF’s are once created they can be re-used on several DataFrame’s and SQL expressions.
'''

columns = ["Seqno","Name"]
data = [("1", "john jones"),
    ("2", "tracey smith"),
    ("3", "amy sanders")]

df = spark.createDataFrame(data=data,schema=columns)

df.show(truncate=False)

#NOW CREATE A PYTRHON FUNCTION convertCase() which takes a string parameter and converts the first letter
# of every word to capital letter. UDF’s take parameters of your choice and returns a value.
def convertCase(str):
    resStr=""
    arr = str.split(" ")
    for x in arr:
       resStr= resStr + x[0:1].upper() + x[1:len(x)] + " "
    return resStr
#Now convert this function convertCase() to UDF by passing the function to PySpark SQL udf(),

""" Converting function to UDF   The default type of the udf() is StringType hence, you can also write
 the above statement without return type."""
convertUDF = udf(lambda z: convertCase(z),StringType())

#NOW USING IT

df.select(col("Seqno"), \
    convertUDF(col("Name")).alias("Name") ) \
   .show(truncate=False)
#df.select(df['Seqno'],convertUDF(df['Name'].alias("Name") )).show(truncate=False)

"""Using UDF with PySpark DataFrame withColumn()"""
def upperCase(str):
    return str.upper()

upperCaseUDF = udf(lambda z:upperCase(z),StringType())

df.withColumn("Cureated Name", upperCaseUDF(col("Name"))) \
  .show(truncate=False)

"""Registering PySpark UDF & use it on SQL"""
""" Using UDF on SQL """
spark.udf.register("convertUDF", convertCase,StringType())
df.createOrReplaceTempView("NAME_TABLE")
spark.sql("select Seqno, convertUDF(Name) as Name from NAME_TABLE") \
     .show(truncate=False)

""" null check """

columns = ["Seqno", "Name"]
data = [("1", "john jones"),
        ("2", "tracey smith"),
        ("3", "amy sanders"),
        ('4', None)]

df2 = spark.createDataFrame(data=data, schema=columns)
df2.show(truncate=False)
df2.createOrReplaceTempView("NAME_TABLE2")
#CHeking null before Passing
spark.udf.register("_nullsafeUDF", lambda str: convertCase(str) if not str is None else "", StringType())

spark.sql("select _nullsafeUDF(Name) from NAME_TABLE2") \
    .show(truncate=False)

spark.sql("select Seqno, _nullsafeUDF(Name) as Name from NAME_TABLE2 " + \
          " where Name is not null and _nullsafeUDF(Name) like '%John%'") \
    .show(truncate=False)