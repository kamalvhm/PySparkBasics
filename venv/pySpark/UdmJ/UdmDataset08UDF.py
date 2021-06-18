from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format,udf
import pyspark.sql.functions as f
from pyspark.sql.types import BooleanType,StringType

#https://amiradata.com/pyspark-groupby-aggregate-data-in-pyspark/
spark=SparkSession.builder.appName("bigLog").master("local").getOrCreate()

def haspass(grade,subject):
        if subject=='Gio':
            if grade.startswith('A'):
                return True
            else:
                return False
        return grade.startswith('A') or grade.startswith('B')



df=spark.read.csv("D:/PackUp/PySparkBasics/venv/pySpark/resources/student.csv",header=True,inferSchema=True)

converUDF=udf(lambda a,b:haspass(a,b),BooleanType())
df.withColumn('isPass',converUDF('grade','subject')).show()


#https://sparkbyexamples.com/pyspark/pyspark-udf-user-defined-function/
#Registering PySpark UDF & use it on SQL

def capital(str):
    return str.upper()

upperConvertUDF=udf(lambda x:capital(x),StringType())
spark.udf.register('upperUdf',upperConvertUDF)
df.createOrReplaceTempView('STUDENT')
spark.sql("select upperUdf(subject) from STUDENT").show()