from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType

#D:/PackUp/PySparkBasics/venv/UDM/resources/people.json
spark=SparkSession.builder.appName("r").master("local").getOrCreate();
df=spark.read.json("D:/PackUp/PySparkBasics/venv/UDM/resources/people.json")
df.show()
df.printSchema()
# its a attribute so no () brakets  , TO fetch only columns returns list['age', 'name']
print(df.columns)
#it returns a dataframe with statistical summary need to call .show() to view df have min max and Standered Diviation
df.describe().show()
#here we have small data so schema is infered correctly long and string but sometime we need to provide schema details for data
data_schema=[
    StructField("age",IntegerType(),True),
    StructField("name",StringType(),True)
]
schema=StructType(data_schema)
df=spark.read.json("D:/PackUp/PySparkBasics/venv/UDM/resources/people.json",schema=schema)
df.printSchema()
#if we need to get a column from this df we can use index call just like in dictionary
print(df['age'])        #we will get column object
# if we need df of just this single column we can use select
df.select("age").show()
#if we need first two rows,we will get list of row obect so we can use index here 'df.head(2)[0]'
print(df.head(2))
#select multiple column we can pass in list in select return df
df.select(['age','name']).show()
#we can add a new column by with column
df.withColumn('doubleCol',df['age']*2).show()
#Renaming Existing COlumn
df.withColumnRenamed('age','newAge').show()
#to create temporaryView
df.createOrReplaceTempView("people")
spark.sql("SELECT * FROM people where age =30").show()

