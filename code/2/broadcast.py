from pyspark.sql import SparkSession
from time import time
from pyspark import RDD

spark = SparkSession.builder.appName("broadcast").getOrCreate()
sc = spark.sparkContext


def split_genres(x):
    x = x.split(",")
    return (x[0], x[1])

def split_ratings(x):
    x = x.split(",")
    return (x[1], (x[0], x[2], x[3]))


def broadcast_join(self, right):
    # hmap:
    # key   = movie_id
    # value = [(movie_id, genre) ... (movie_id, genre)]
    hmap = sc.broadcast(right.map(lambda r: (r[0], r)).groupByKey().collectAsMap())
    
    rdd = self.flatMap(lambda x: [(x[0],(r[1],x[1])) for r in hmap.value.get(x[0], [])])

    return rdd


rdd1 = sc.textFile("hdfs://master:9000/movie_data/ratings.csv").map(lambda x: split_ratings(x))
rdd2 = sc.textFile("hdfs://master:9000/movie_data/genres100.csv").map(lambda x: split_genres(x))

RDD.broadcast_join = broadcast_join

t1 = time()
rdd = rdd1.broadcast_join(rdd2).collect()
t2 = time()

print("-------------------------------------------------------------")
print("Broadcast Join Output")
for i in rdd:
    print(i)
print("-------------------------------------------------------------")
print('Time: ' + str(t2-t1))
print("-------------------------------------------------------------")