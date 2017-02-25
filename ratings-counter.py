import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
import collections
import os
os.getcwd()
os.chdir("C:\pyspark")
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
#sc = SparkContext(conf = conf)

lines = sc.textFile("file:///pyspark/ml-100k/u.data")
ratings = lines.map(lambda x: (x.split()[2], 1))

    result = ratings.reduceByKey(lambda x,y : x+y).sortByKey().collectAsMap()

#sortedResults = collections.OrderedDict(sorted(result.items()))
#for key, value in sortedResults.items():
#    print("%s %i" % (key, value))

import matplotlib.pyplot as plt
import seaborn as sns

fig = plt.figure(figsize=(12,8))
#plt.bar(sortedResults.keys(), sortedResults.values(), align='center')
plt.bar(range(len(result)), result.values(), align='center')
plt.xticks(range(len(result)), result.keys())

plt.tight_layout()
plt.show()
