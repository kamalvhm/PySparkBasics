from itertools import product

from pyspark.sql import SparkSession


spark=SparkSession.builder.appName("Excerscise").master("local").getOrCreate()

df=spark.read.csv("D:/PackUp/PySparkBasics/venv/UDM/resources/walmart_stock.csv",header=True,inferSchema=True)
df.show(10)

#What are th column Names


print("Q1)What does schema look like")
df.printSchema()

print("Q2)Print out first 5 Rows")
print(df.head(5))

print("Q3)Describe")
newDf=df.describe().show()

print("Q3)Bonus Question")
from pyspark.sql.functions import format_number
result=df.describe()

result.select(result['summary'], format_number(result['Open'].cast('float'),2).alias('Open')).show()

print("Q4)Create a new dataframe with a column called HV Ratio that is the ratio of the High Price versus volume of stock traded for a day.")
df.withColumn("HV Ratio",df['High']/df['Volume']).show()


print("Q5)What Day had The Peak high in price")
print(df.orderBy(df['High'].desc()).head(1)[0][0])

print("Q6)What is the mean of the Close column?")

print("Q7)What is the max and min of the Volume column?")


print("Q8)How many days was the Close lower than 60 dollars?")

import pyspark.sql.functions as f
result=result.filter("close<60").count()
print("Q9)What percentage of the time was the High greater than 80 dollars ? In other words, (Number of Days High>80)/(Total Days in the dataset)")
print((df.filter(df['High']>80).count()/df.count())*100)

print("Q10)What is the Pearson correlation between High and Volume?")
df.select(f.corr(df['High'],df["Volume"])).show()


print("Q10)What is the max High per year?")
newDf=df.withColumn("year",f.year(df['Date']))
newDf.groupBy("year").max().show()

#### What is the average Close for each Calendar Month?
#### In other words, across all the years, what is the average Close price for Jan,Feb, Mar, etc... Your result will have a
# value for each of these months.
