from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("MISS").master("local").getOrCreate();
"""
YOU HAVE THREE OPTIONS TO HANDLE MISSING DATA
    1)KEEP IT AS IT IS
    2)DROP MISSING DATA POINT INCLUDING ROW
    3) REPLACE THEM WITH OTHER VALUES
"""
df=spark.read.csv('D:/PackUp/PySparkBasics/venv/UDM/resources/ContainsNull.csv',inferSchema=True,header=True)

df.show()


#2) DROP DATA NA will drop all row which contains null
df.na.drop().show()
#thresh=2 this will alow 1 null value if its more than equal 2 it will remove
df.na.drop(thresh=2).show()
df.na.drop(how="all").show() #if all null then removed by default how="Any"
#you can pass subset of col to look for null SO it will look only Sales
df.na.drop(subset=['Sales']).show()

#3) IF WE NEED TO FILL VALUE INSTEAD OF DROP
df.na.fill('FILL VALUE').show() # as we difined str in fill it will fill only for StringType not for  Integers (Sales)
df.na.fill(0).show()
df.na.fill("No Name",subset=['Name']).show() # we can specify target column also
#Now if we need to fill missing Sales value as mean value of column
#from pyspark.sql.functions import mean
#meanVal=df.salect(mean(df[''Sales])).collect()
#mean_sales=meanVal[0][0]
#df.fill(mean_sales,['Sales']).show()

#FOR MULTIPLE VALUES
df.na.fill({"city": "unknown", "type": ""}) \
    .show()