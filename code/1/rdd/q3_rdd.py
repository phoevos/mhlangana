from pyspark.sql import SparkSession
from time import time
from io import StringIO
import csv

#returns (movie_id, genre)
def split_movie_genres(x):
    x = x.split(',')
    return x

#returns (movie_id, rating)
def split_ratings(x):
    x = x.split(',')
    return (x[1], float(x[2]))

spark = SparkSession.builder.appName("q3-rdd").getOrCreate()

sc = spark.sparkContext

t1 = time()
ratings = sc.textFile("hdfs://master:9000/movie_data/ratings.csv").\
          map(lambda x: split_ratings(x))
movie_genres = sc.textFile("hdfs://master:9000/movie_data/movie_genres.csv").\
               map(lambda x: split_movie_genres(x))

joined_data = ratings.\
              map(lambda x: (x[0], (x[1], 1))).\
              reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).\
              map(lambda x: (x[0], x[1][0]/x[1][1])).\
              join(movie_genres).\
              map(lambda x: (x[1][1], (x[1][0], 1))).\
              reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1])).\
              map(lambda x: (x[0], (x[1][0]/x[1][1], x[1][1]))).collect()

# 2nd map:
# key   = movie_id
# value = avg_rating

# join:
# key   = movie_id
# value = (avg_rating, genre) 

# 3rd map:
# key   = genre
# value = (avg_rating, 1)

# 4th map:
#   key = genre
#   value = (avg_genre_rating, numOfMoviesPerGenre)

t2 = time()

for t in joined_data:
	print(t)

print('-------------------------------------')
print('Time: ' + str(t2-t1))
print('-------------------------------------')

