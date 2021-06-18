from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
lst=[("Maxy brown",23),("miky",40)]
df =spark.createDataFrame(lst,["name","age",])
df.show()

print("1) asc")
df.sort(df.age.desc()).show()

print("2) starts with")


print("3)like")

print("4)between")

print("5)isIn")
df.select("")
print("6)isNotNull")
print("7)contains")
print("8)substr")

print("9)when and otherwise")





