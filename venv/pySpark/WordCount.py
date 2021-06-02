from pyspark import SparkContext,SparkConf

conf=SparkConf().setAppName("WC").setMaster("local[*]")
sc=SparkContext(conf=conf)

wordRdd=sc.textFile("resources/words.txt")
wordRdd=wordRdd.flatMap(lambda line:line.split(' '))
print("Type Before ",type(wordRdd))
wordRdd=wordRdd.map(lambda word:(word,1)) #pythn is not type safe so (word,1) will change type automatically
print("Type After  ",type(wordRdd))
print("",wordRdd.collect())
wordRdd=wordRdd.reduceByKey(lambda a,b:a+b)

print(wordRdd.collect())