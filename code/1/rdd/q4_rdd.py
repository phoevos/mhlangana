from pyspark.sql import SparkSession
from time import time
from io import StringIO
import csv

#returns (movie_id, genre)
def split_movie_genres(x):
    x = x.split(',')
    return x

#returns (movie_id, (summaryLength, year))
def split_movies(x):
    l = list(csv.reader(StringIO(x),delimiter=','))[0]
    l[3] = '0000' if (l[3] == '') else l[3]
    return (l[0], (summaryLength(l[2]), quinquennium(l[3][0:4])))

def summaryLength(summary):
    words = summary.split()
    return len(words)

def quinquennium(year):
    quint = (int(year)-2000)//5 + 1 if (year > '1999') else 0
    return (str(quint) + "η 5ετία")

spark = SparkSession.builder.appName("q4-rdd").getOrCreate()

sc = spark.sparkContext

t1 = time()

movies = sc.textFile("hdfs://master:9000/movie_data/movies.csv").\
         map(lambda x: split_movies(x)).\
         filter(lambda x: ((x[1][0] != 0) and (x[1][1] != '0η 5ετία')))

movie_genres = sc.textFile("hdfs://master:9000/movie_data/movie_genres.csv").\
               map(lambda x: split_movie_genres(x)).\
               filter(lambda x: x[1] == 'Drama')

# key = movie_id
# value = ((summaryLength, quinquennium), genre)
joined_data = movies.join(movie_genres).\
              map(lambda x: (x[1][0][1], (x[1][0][0], 1))).\
              reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).\
              map(lambda x: (x[0], x[1][0]/x[1][1])).\
              sortByKey().collect()

# 1st map:
# key = quinquennium
# value (summaryLength, 1)

# 2nd map:
# key = quinquennium
# value = avg_summaryLength

t2 = time()

for t in joined_data:
    print(t)

print('-------------------------------------')
print('Time: ' + str(t2-t1))
print('-------------------------------------')

