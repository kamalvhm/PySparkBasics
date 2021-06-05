from pyspark import SparkContext,SparkConf

conf=SparkConf().setAppName("WC").setMaster("local[*]")
sc=SparkContext(conf=conf)

wordRdd=sc.textFile("resources/words.txt")
wordRdd=wordRdd.flatMap(lambda line:line.split(' '))
wordRdd=wordRdd.filter(lambda line:len(line)>0)

wordRdd=wordRdd.map(lambda word:(word,1)) #pythn is not type safe so (word,1) will change type automatically TO PairRDDFunctions

wordRdd=wordRdd.reduceByKey(lambda a,b:a+b)

wordRdd=wordRdd.map(lambda x:(x[1],x[0])).sortByKey(False);  #Fliping String and Integer

#print(wordRdd.collect(),end="")
wordRdd.foreach(lambda n:print(n))
# To Save to some file
#wordRdd.saveAsTextFile("resources/wordsOut")