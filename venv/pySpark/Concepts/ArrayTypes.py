from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,StructField,ArrayType,StructType

spark=SparkSession.builder.appName("at").master("local").getOrCreate()


data = [
 ("James,,Smith",["Java","Scala","C++"],["Spark","Java"],"OH","CA"),
 ("Michael,Rose,",["Spark","Java","C++"],["Spark","Java"],"NY","NJ"),
 ("Robert,,Williams",["CSharp","VB"],["Spark","Python"],"UT","NV")
]

schema = StructType([
    StructField("name",StringType(),True),
    StructField("languagesAtSchool",ArrayType(StringType()),True),
    StructField("languagesAtWork",ArrayType(StringType()),True),
    StructField("currentState", StringType(), True),
    StructField("previousState", StringType(), True)
  ])

df = spark.createDataFrame(data=data,schema=schema)
#df.printSchema()
df.show()

#PySpark ArrayType (Array) Functions

"""explode()  -like FlatMap on list
Use explode() function to create a new row for each element in the given array column. There are various PySpark SQL 
explode functions available to work with Array columns."""

from pyspark.sql.functions import explode
df.select(df.name,explode(df.languagesAtSchool)).show()

"""Split()
split() sql function returns an array type after splitting the string column by delimiter. Below example split the name
 column by comma delimiter."""

from pyspark.sql.functions import split
df.select(split(df.name,",").alias("nameAsArray")).show()

"""array()
Use array() function to create a new array column by merging the data from multiple columns. All input columns must have
 the same data type. The below example combines the data from currentState and previousState and creates a new column states."""

from pyspark.sql.functions import array
df.select(df.name,array(df.currentState,df.previousState).alias("States"))

"""array_contains()
array_contains() sql function is used to check if array column contains a value. Returns null if the array is null, true
 if the array contains the value, and false otherwise."""

from pyspark.sql.functions import array_contains
df.select(df.name,array_contains(df.languagesAtSchool,"Java")
    .alias("array_contains")).show()