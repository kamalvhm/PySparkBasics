from pyspark.sql import SparkSession
import pyspark.sql.functions as f

spark=SparkSession.builder.appName("pivot").master("local").getOrCreate()

'''Spark pivot() function is used to pivot/rotate the data from one DataFrame/Dataset column into multiple columns
 (transform row to column) and unpivot is used to transform it back (transform columns to rows).'''

data=[("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico")]

df=spark.createDataFrame(data).toDF("Product","Amount","Country")
df.show()
''' It is an aggregation where one of the grouping columns values transposed into individual columns with distinct data.
 From the above DataFrame, to get the total amount exported to each country of each product will do group by Product, 
 pivot by Country, and the sum of Amount.'''

pivotDF=df.groupBy("Product").pivot("Country").sum("Amount")
pivotDF.show()

'''Spark 2.0 on-wards performance has been improved on Pivot, however, if you are using lower version; note that pivot is
 a very expensive operation hence, it is recommended to provide column data (if known) as an argument to function as shown below.'''

countries = ["USA","China","Canada","Mexico"]
pivotDF = df.groupBy("Product").pivot("Country", countries).sum("Amount")
pivotDF.show()
'''Unpivot is a reverse operation, we can achieve by rotating column values into rows values. Spark SQL doesn’t have unpivot function 
hence will use the stack() function. Below code converts column countries to row.'''
pivotDF.select("Product",
f.expr("stack(3, 'Canada', Canada, 'China', China, 'Mexico', Mexico) as (Country,Total)"))\
    .where("Total is not null").show()
