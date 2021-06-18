from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("example").master("local").getOrCreate()
"""PySpark filter() function is used to filter the rows from RDD/DataFrame based on the given condition or SQL expression,
 you can also use where() clause instead of the filter() if you are coming from an SQL background, both these functions operate 
 exactly the same."""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, ArrayType

data = [
    (("James", "", "Smith"), ["Java", "Scala", "C++"], "OH", "M"),
    (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "NY", "F"),
    (("Julia", "", "Williams"), ["CSharp", "VB"], "OH", "F"),
    (("Maria", "Anne", "Jones"), ["CSharp", "VB"], "NY", "M"),
    (("Jen", "Mary", "Brown"), ["CSharp", "VB"], "NY", "M"),
    (("Mike", "Mary", "Williams"), ["Python", "VB"], "OH", "M")
]

schema = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('lastname', StringType(), True)
    ])),
    StructField('languages', ArrayType(StringType()), True),
    StructField('state', StringType(), True),
    StructField('gender', StringType(), True)
])

df = spark.createDataFrame(data=data, schema=schema)
#df.printSchema()
df.show(truncate=False)

"""1. DataFrame filter() with Column Condition"""


# Using equals condition
df.filter(df.state == "OH").show(truncate=False)

# not equals condition
df.filter(df.state != "OH") \
    .show(truncate=False)
df.filter(~(df.state == "OH")) \
    .show(truncate=False)

#Using SQL col() function
from pyspark.sql.functions import col
df.filter(col("state") == "OH") \
    .show(truncate=False)

"""2. DataFrame filter() with SQL Expression"""
#Using SQL Expression
df.filter("gender == 'M'").show()
#For not equal
df.filter("gender != 'M'").show()
df.filter("gender <> 'M'").show()

"""3. PySpark Filter with Multiple Conditions"""
df.filter( (df.state  == "OH") & (df.gender  == "M") ) \
    .show(truncate=False)

"""4. Filter Based on List Values"""
#Filter IS IN List values
li=["OH","CA","DE"]
df.filter(df.state.isin(li)).show()

"""5. Filter Based on Starts With, Ends With, Contains"""

# Using startswith
df.filter(df.state.startswith("N")).show()
# +--------------------+------------------+-----+------+
# |                name|         languages|state|gender|
# +--------------------+------------------+-----+------+
# |      [Anna, Rose, ]|[Spark, Java, C++]|   NY|     F|
# |[Maria, Anne, Jones]|      [CSharp, VB]|   NY|     M|
# |  [Jen, Mary, Brown]|      [CSharp, VB]|   NY|     M|
# +--------------------+------------------+-----+------+

#using endswith
df.filter(df.state.endswith("H")).show()

#contains
df.filter(df.state.contains("H")).show()

"""6. PySpark Filter like and rlike"""
"""If you have SQL background you must be familiar with like and rlike (regex like), PySpark also provides similar methods
 in Column class to filter similar values using wildcard characters. You can use rlike() to filter by checking values case insensitive."""

data2 = [(2,"Michael Rose"),(3,"Robert Williams"),
     (4,"Rames Rose"),(5,"Rames rose")
  ]
df2 = spark.createDataFrame(data = data2, schema = ["id","name"])

# like - SQL LIKE pattern
df2.filter(df2.name.like("%rose%")).show()
# +---+----------+
# | id|      name|
# +---+----------+
# |  5|Rames rose|
# +---+----------+

# rlike - SQL RLIKE pattern (LIKE with Regex)
#This check case insensitive
df2.filter(df2.name.rlike("(?i)^*rose$")).show()
# +---+------------+
# | id|        name|
# +---+------------+
# |  2|Michael Rose|
# |  4|  Rames Rose|
# |  5|  Rames rose|

"""7. Filter on an Array column"""
##When you want to filter rows from DataFrame based on value present in an array collection column, you can use the first
# syntax. The below example uses array_contains()
from pyspark.sql.functions import array_contains
df.filter(array_contains(df.languages,"Java")) \
    .show(truncate=False)

"""9. Filtering on Nested Struct columns"""
  #Struct condition
df.filter(df.name.lastname == "Williams") \
    .show(truncate=False)