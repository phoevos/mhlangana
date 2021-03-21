from pyspark.sql import SparkSession
from time import time
from io import StringIO
import csv

# returns (movie_id, (title, popularity))
def split_movies(x):
    l = list(csv.reader(StringIO(x),delimiter=','))[0]
    return (l[0], (l[1], float(l[7])))

# returns (movie_id, genre)
def split_movie_genres(x):
    x = x.split(',')
    return (x[0],x[1])

# returns (movie_id, (user_id, rating))
def split_ratings1(x):
    x = x.split(',')[0:3]
    return (x[1], (x[0], float(x[2])))

# returns (user_id, (movie_id, rating))
def split_ratings2(x):
    x = x.split(',')[0:3]
    return (x[0], (x[1], float(x[2])))

spark = SparkSession.builder.appName("q5-rdd").getOrCreate()

sc = spark.sparkContext

t1 = time()

movies = sc.textFile("hdfs://master:9000/movie_data/movies.csv").\
         map(lambda x: split_movies(x))

movie_genres = sc.textFile("hdfs://master:9000/movie_data/movie_genres.csv").\
               map(lambda x: split_movie_genres(x))

ratings = sc.textFile("hdfs://master:9000/movie_data/ratings.csv")
ratings1 = ratings.map(lambda x: split_ratings1(x))     
ratings2 = ratings.map(lambda x: split_ratings2(x))

users = ratings1.join(movie_genres).\
        map(lambda x: ((x[1][1], x[1][0][0]), (1, x[1][0][1], x[1][0][1]))).\
        reduceByKey(lambda x,y: (x[0]+y[0], x[1] if x[1] > y[1] else y[1], x[2] if x[2] < y[2] else y[2])).\
        map(lambda x: (x[0][0], (x[0][1], x[1][0], x[1][1], x[1][2]))).\
        reduceByKey(lambda x,y: x if x[1] > y[1] else y).\
        map(lambda x: (x[1][0], (x[0], x[1][1], x[1][2], x[1][3]))) 

# join:
# key = movie_id
# value = ((user_id, rating), genre)

# 1st map:
# key   = (genre, user_id)
# value = (1, rating)

# 2nd map:
# key   = genre
# value = (user_id, reviews, max_rating, min_rating)

# 3rd map:
# key   = user_id
# value = (genre, max_reviews, max_rating, min_rating)

movie_genres = movie_genres.map(lambda x: (x, ()))

joined_data = ratings2.join(users).\
              filter(lambda x: x[1][0][1] == x[1][1][2] or x[1][0][1] == x[1][1][3]).\
              map(lambda x: (x[1][0][0], (x[0], x[1][0][1], x[1][1][0], x[1][1][1]))).\
              join(movies).\
              map(lambda x: ((x[0], x[1][0][2]), (x[1][0][0], x[1][0][1], x[1][0][3], x[1][1][0], x[1][1][1]))).\
              join(movie_genres).\
              map(lambda x: ((x[0][1], x[1][0][0], x[1][0][2]), ((x[1][0][3], x[1][0][1], x[1][0][4]), (x[1][0][3], x[1][0][1], x[1][0][4])))).\
              reduceByKey(lambda x,y: (x[0] if x[0][1]>y[0][1] or (x[0][1]==y[0][1] and x[0][2]>y[0][2]) else y[0], x[1] if x[1][1]<y[1][1] or (x[1][1]==y[1][1] and x[1][2]>y[1][2]) else y[1])).\
              map(lambda x: (x[0][0], (x[0][1], x[0][2], x[1][0][0], x[1][0][1], x[1][1][0], x[1][1][1]))).\
              sortByKey().collect()

# join:
# key   = user_id
# value = ((movie_id, rating), (genre, max_reviews, max_rating, min_rating))

# 1st map:
# key   = movie_id
# value = (user_id, rating, genre, max_reviews)

# join:
# key   = movie_id
# value = ((user_id, rating, genre, max_reviews), (title, popularity))

# 2nd map:
# key   = (movie_id, genre)
# value = (user_id, rating, max_reviews, title, popularity)

# join:
# key   = (movie_id, genre)
# value = ((user_id, rating, max_reviews, title, popularity), ())

# 3rd map:
# key   = (genre, user_id, max_reviews)
# value = ((title, rating, popularity), (title, rating, popularity))

# 4th map:
# key   = genre 
# value = (user_id, max_reviews, title, rating, title, rating)

t2 = time()

for t in joined_data:
  print(t)
 
print('-------------------------------------')
print('Time: ' + str(t2-t1))
print('-------------------------------------')


