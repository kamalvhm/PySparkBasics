from pyspark.sql import SparkSession,udf
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import date_format,dayofmonth,month,year,format_number
spark=SparkSession.builder.appName("fromJava01").master("local").getOrCreate()
df=spark.read.csv("D:/PackUp/PySparkBasics/venv/pySpark/resources/student.csv",header=True,inferSchema=True)

modernArtResults=df.filter("subject ='Modern Art' AND year >=2007")
modernArtResults=df.filter((df.subject=='Modern Art') & (df.year>=2007))
df.createOrReplaceTempView("students")
spark.sql("select avg(score) from students where subject ='English'").show()
        #CREATING DF FROM COLLECTION
lst=[("WARN","2016-12-31 04:19:32"),("WARN","2016-12-31 03:21:21"),("FATAL","2016-12-31 03:22:34")
     ,("INFO","2015-04-21 14:32:21"),("FATAL","2015-04-21 19:23:20")]
schema=StructType([StructField("level",StringType(),True), StructField("date",StringType(),True)])
df.createOrReplaceTempView("logging")
df=spark.sql("select level,collect_list(date) from logging group by level order by level")
            #########To COUNT BY MONTH
df.createOrReplaceTempView("logging")
df=spark.sql("select level,date_format(date,'MMMM') as month from logging")
result=spark.sql("select level,month,count(1) as total from logging group by level,month")
            #######WITH MONTH NUM SAME COUNT BY MONTH
df=spark.sql("select level,date_format(datetime,'MMMM') as month ,cast(first(date_format(datetime,'M')) as int) monthnum,count(1) as total "
             "from logging_table group by level,month order by monthnum")
            #SAME BY FUNCTIONS"""
df=df.select(['level',date_format('datetime',"MMMM").alias("month"), date_format('datetime','M').alias('monthNum').cast('integer')])
df=df.groupBy("level","monthNum").count()
            #PIVOT TABLE
lst=["January","February","March","April","May","June","july","August","September","October","November","December"]
df=df.groupBy("level").pivot("month",lst).count().na.fill(0)
            #UDF
def capital(str):
    return str.upper()
upperConvertUDF=udf(lambda x:capital(x),StringType())
df.withColumn('isPass',upperConvertUDF('subject')).show()
spark.udf.register('upperUdf',upperConvertUDF)  ########REGISTER ONLY FOR SQL
df.createOrReplaceTempView('STUDENT')
spark.sql("select upperUdf(subject) from STUDENT").show()
#-----------------------------------UDM2 Some BsicOPs--------print(df['age'])---df.select("age")--print(df.head(2))--df.select(['age','name']).show()--df.withColumn('doubleage',df['age']*2).show()
#--df.withColumnRenamed('age','newAge').show() -----df.filter(df['Close']<500).select("Volume").show()
#GROUP BY AGGREGATE
df.groupBy("Company").mean().show()
df.agg({'Sales':'sum'}).show()
#Both at once
group_data=df.groupBy("Company")
group_data.agg({'Sales':'max'}).show() #ORDER BY df.orderBy(df['Sales'].desc()).show()
#MISSING DATA df.na.drop().show() | df.na.drop(subset=['Sales']).show()| df.na.drop(thresh=2).show() | df.na.drop(how="all").show()
#df.na.fill('FILL VALUE').show()  | df.na.fill("No Name",subset=['Name']).show()
df.select(dayofmonth(df['Date'])).show()
df.select(month(df['Date'])).show()
#Average Closing price per year
#df.select(year(df['Date'])).show()
newDf=df.withColumn("Year",year(df['date'])) ####################################DATE TIME#################################
result=newDf.groupBy("Year").mean().select(['Year','avg(Close)'])
result=result.withColumnRenamed('avg(Close)','Average Closing Price')
result.select(['year',format_number('Average Closing Price',2).alias("avg Close")]).show()