
from pyspark import SparkConf, SparkContext


#sc = SparkContext(conf = conf)

inputrdd = sc.textFile("file:///pyspark/customer-orders.csv")
custamountrdd = inputrdd.map(lambda x: (x.split(","))).map(lambda x: (int(x[0]) , float(x[2])))
custtotalrdd = custamountrdd.reduceByKey(lambda x, y : x+ y)

flipped = custtotalrdd.map(lambda x :(x[1],x[0]))
sorted = flipped.sortByKey()
customerdict = sorted.collect()

for item in customerdict:
    print (item)
