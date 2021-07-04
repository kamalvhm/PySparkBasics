from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType,StringType,IntegerType

spark=SparkSession.builder.appName("wc").master("local").getOrCreate();


