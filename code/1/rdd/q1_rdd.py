from pyspark.sql import SparkSession
from time import time
from io import StringIO
import csv

#returns (title, timestamp, cost, income)
def split_movies(x):
  l = list(csv.reader(StringIO(x),delimiter=','))[0]
  return (l[1], l[3], l[5], l[6])

def getProfit(cost, inc):
  return (inc - cost)*100/cost if cost else 0

spark = SparkSession.builder.appName("q1-rdd").getOrCreate()

sc = spark.sparkContext

t1 = time()

rdd = sc.textFile("hdfs://master:9000/movie_data/movies.csv").map(lambda x: split_movies(x)).\
      filter(lambda x: (int(x[1][0:4])>=2000 if x[1] and int(x[2])and int(x[3]) else None)).\
      map(lambda x: (int(x[1][0:4]),(x[0], getProfit(int(x[2]),int(x[3]))))).reduceByKey(lambda x, y: x if x[1] >= y[1] else y).\
      sortByKey().collect()

# 1st map: 
#   key   = year 
#   value = (title, profit) 

t2 = time()

for t in rdd:
  print(t)

print('-------------------------------------')
print('Time: ' + str(t2-t1))
print('-------------------------------------')

