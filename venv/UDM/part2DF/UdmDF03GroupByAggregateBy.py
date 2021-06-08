from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("ops").master("local").getOrCreate()

df=spark.read.csv('D:/PackUp/PySparkBasics/venv/UDM/resources/appl_stock.csv',inferSchema=True,header=True)

df.show()
df.printSchema()
# if we need data which have closing price <500
df.filter("Close<500").show()
# we can combine this with select df.filter("close<500").select(['Open','Close']).show()
#Same with internal methods instead of sql syntax
df.filter(df['Close']<500).select("Volume").show()
#filter on multiple Conditions - Open price >200 and CLose price < 200
#The WRONG ONE WITH AND df.filter(df['CLose']<200 and df['OPen']>200) ,WILL NOT WORK
df.filter((df['CLose'] < 200) & (df['OPen'] > 200)).show() #  for not > 200 '~(df['OPen'] > 200)'
#what day the Low price was 170.16
df.filter(df['Low']==170.16).show() # to save in list use collect() result=df.filter(df['Low']==170.16).collect()
result=df.filter(df['Low']==170.16).collect()
row=result[0]#grab first record
row.asDict()['Volumn']#convert it to dictionary


