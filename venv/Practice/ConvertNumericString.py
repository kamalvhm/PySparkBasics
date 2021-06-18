from pyspark.sql import SparkSession,Row,Column
from pyspark.sql.types import StringType
from pyspark.sql.functions import col,udf



spark=SparkSession.builder.appName("create DF").master("local").getOrCreate()


df=spark.read.csv("D:/PackUp/PySparkBasics/venv/Practice/numo.csv",header=True)
df.show()

help_dict={"one":"1","two":"2","three":"3","four":'4"',"five":"5","six":"6","seven":"7","eight":"8","nine":"9"}


def convert(input):
    if type(input)==str:
        res = "".join(help_dict[ele] for ele in input.split(" "))
        res = res.replace('"', '')
        return res.strip()
    return input

convertedUDF=udf(lambda x:convert(x),StringType())

df=df.withColumn("newAge",convertedUDF(df["age"]))
df.show()