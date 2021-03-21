from pyspark.sql import SparkSession
from time import time
from io import StringIO
import csv

#returns (user_id, (rating, 1))
def split_ratings(x):
    x = x.split(',')
    return (x[0], (float(x[2]), 1))

spark = SparkSession.builder.appName("q2-rdd").getOrCreate()

sc = spark.sparkContext

t1 = time()
rdd = sc.textFile("hdfs://master:9000/movie_data/ratings.csv").\
      map(lambda x: split_ratings(x)).\
      reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).\
      map(lambda x: (x[0],x[1][0]/x[1][1]))
total = rdd.count()
count = rdd.filter(lambda x: x[1]>3.0).count()
t2 = time()

print(count*100/total)

print('-------------------------------------')
print('Time: ' + str(t2-t1))
print('-------------------------------------')

