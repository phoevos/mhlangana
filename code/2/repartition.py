from pyspark.sql import SparkSession
from time import time
from pyspark import RDD

spark = SparkSession.builder.appName("repartition").getOrCreate()
sc = spark.sparkContext

def split_genres(x):
    x = x.split(",")
    return (x[0], x[1])

def split_ratings(x):
    x = x.split(",")
    return (x[1], (x[0], x[2], x[3]))

def repartition_reduce(seq):
    lbuf, rbuf = [], []
    recs = seq[1]
    for (tag, rec) in recs:
        if tag == 'L':
            lbuf.append(rec)
        else:
            rbuf.append(rec)

    return ((seq[0], (l, r)) for l in lbuf for r in rbuf)


def repartition_join(self, right):
    left = self.map(lambda x: (x[0], ('L', x[1])))
    right = right.map(lambda x: (x[0], ('R', x[1])))
    rdd = left.union(right).groupByKey().flatMap(lambda x: repartition_reduce(x))
    return rdd

rdd1 = sc.textFile("hdfs://master:9000/movie_data/genres100.csv").map(lambda x: split_genres(x))

rdd2 = sc.textFile("hdfs://master:9000/movie_data/ratings.csv").map(lambda x: split_ratings(x))

RDD.repartition_join = repartition_join

t1 = time()
rdd = rdd1.repartition_join(rdd2).collect()
t2 = time()

print("-------------------------------------------------------------")
print("Repartition Join Output")
for i in rdd:
    print(i)
print("-------------------------------------------------------------")
print('Time: ' + str(t2-t1))
print("-------------------------------------------------------------")
