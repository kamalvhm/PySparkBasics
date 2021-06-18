from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()

data = [
    ("James",None,"M"),
    ("Anna","NY","F"),
    ("Julia",None,None)
  ]

columns = ["name","state","gender"]
df = spark.createDataFrame(data,columns)
df.show()

"""
While working on PySpark SQL DataFrame we often need to filter rows with NULL/None values on columns,
you can do this by checking IS NULL or IS NOT NULL conditions.

In PySpark, using filter() or where() functions of DataFrame we can filter rows with NULL values by
checking isNULL() of PySpark Column class.
"""
df.filter("state is NULL").show()
#df.filter(df.state.isNull()).show()
#df.filter(col("state").isNull()).show()

#On multiple Column
df.filter("state IS NULL AND gender IS NULL").show()
#df.filter(df.state.isNull() & df.gender.isNull()).show()

#isNotNull() is used to filter rows that are NOT NULL in DataFrame columns.
from pyspark.sql.functions import col
df.filter("state IS NOT NULL").show()
#df.filter("NOT state IS NULL").show()
#df.filter(df.state.isNotNull()).show()
#df.filter(col("state").isNotNull()).show()