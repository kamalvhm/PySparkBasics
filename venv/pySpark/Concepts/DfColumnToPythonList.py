from pyspark.sql import SparkSession
s



spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()

data = [("James","Smith","USA","CA"),("Michael","Rose","USA","NY"), \
    ("Robert","Williams","USA","CA"),("Maria","Jones","USA","FL") \
  ]
columns=["firstname","lastname","country","state"]
df=spark.createDataFrame(data=data,schema=columns)
print(df.collect())

"""1) Selecting one collumn then collect on it """
"""1. Convert DataFrame Column to Python List
As you see above output, PySpark DataFrame collect() returns a Row Type, hence in order to convert DataFrame Column to Python 
List first, you need to select the DataFrame column you wanted using rdd.map() lambda expression and then collect the DataFrame.
"""
states1=df.rdd.map(lambda x: x[3]).collect()
print(states1)

"""1.1 Remove Duplicates from List"""

#Remove duplicates after converting to List
from collections import OrderedDict
res = list(OrderedDict.fromkeys(states1))
print(res)

"""2. Referring Column Name you wanted to Extract :- Here is another alternative of getting a DataFrame column as a Python 
List by referring column name from Row Type.

"""
states2=df.rdd.map(lambda x: x.state).collect()
print(states2)
#['CA', 'NY', 'CA', 'FL']

"""3. Using flatMap() Transformation"""
states4=df.select(df.state).rdd.flatMap(lambda x: x).collect()
print(states4)
#['CA', 'NY', 'CA', 'FL']

"""5. Getting Column in Row Type"""
states3=df.select(df.state).collect()
print(states3)