from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("MISS").master("local").getOrCreate();

df=spark.read.csv('D:/PackUp/PySparkBasics/venv/UDM/resources/appl_stock.csv',inferSchema=True,header=True)

#df.show()

from pyspark.sql.functions import (dayofmonth,hour,dayofyear,
                                   month,year,weekofyear,format_number
                                   ,date_format)
#This will return Day of the Month
df.select(dayofmonth(df['Date'])).show()
df.select(month(df['Date'])).show()

#Average Closing price per year
#df.select(year(df['Date'])).show()
newDf=df.withColumn("Year",year(df['date']))
result=newDf.groupBy("Year").mean().select(['Year','avg(Close)'])

result=result.withColumnRenamed('avg(Close)','Average Closing Price')
result.select(['year',format_number('Average Closing Price',2).alias("avg Close")]).show()