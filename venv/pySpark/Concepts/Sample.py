from pyspark.sql import SparkSession

"""
PySpark provides a pyspark.sql.DataFrame.sample(), pyspark.sql.DataFrame.sampleBy(), RDD.sample(), and RDD.takeSample()
methods to get the random sampling subset from the large dataset, In this article I will explain with Python examples .

sample(withReplacement, fraction, seed=None)

fraction – Fraction of rows to generate, range [0.0, 1.0]. Note that it doesn’t guarantee to provide the exact number of the fraction of records.

seed – Seed for sampling (default a random seed). Used to reproduce the same random sampling.

withReplacement – Sample with replacement or not (default False).
"""

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()

df=spark.range(100)
print(df.sample(0.06).collect())

"""Every time you run a sample() function it returns a different set of sampling records, however sometimes during the 
development and testing phase you may need to regenerate the same sample every time as you need to compare the results from your previous run.
 To get consistent same random sampling uses the same slice value for every run. Change slice value to get different results."""

print(df.sample(0.1,123).collect())
#Output: 36,37,41,43,56,66,69,75,83
print(df.sample(0.1,123).collect())
#Output: 36,37,41,43,56,66,69,75,83
print(df.sample(0.1,456).collect())
#Output: 19,21,42,48,49,50,75,80

"""some times you may need to get a random sample with repeated values. By using the value true, results in repeated values."""

print(df.sample(True,0.3,123).collect()) #with Duplicates
#Output: 0,5,9,11,14,14,16,17,21,29,33,41,42,52,52,54,58,65,65,71,76,79,85,96
print(df.sample(0.3,123).collect()) # No duplicates
#Output: 0,4,17,19,24,25,26,36,37,41,43,44,53,56,66,68