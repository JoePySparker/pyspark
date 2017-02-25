from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
#sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("file:///pyspark/fakefriends.csv")
rdd = lines.map(parseLine)
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
keys= averagesByAge.keys().collect()
values = averagesByAge.values().collect()
#for result in results:
#    print(result)
import matplotlib.pyplot as plt
import seaborn as sns

fig = plt.figure(figsize=(12,8))
plt.plot(keys, values)

plt.tight_layout()
plt.show()
