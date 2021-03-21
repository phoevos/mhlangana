from pyspark.sql import SparkSession
from time import time

spark = SparkSession.builder.appName("query3-sql").getOrCreate()

t1 = time()
movie_genres = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/movie_data/movie_genres.csv")
ratings = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/movie_data/ratings.csv")

movie_genres.registerTempTable("movie_genres")
ratings.registerTempTable("ratings")

sqlString = \
        "select g._c1 as Genre, avg(AvgMovieRating) as AvgGenreRating, count(*) as NumOfMovies " +\
        "from ( "  +\
                "select r._c1 as Movie, avg(r._c2) AvgMovieRating " +\
                "from ratings as r " +\
                "group by r._c1) " +\
        "join movie_genres as g on g._c0 = Movie " +\
        "group by g._c1 " +\
        "order by g._c1"

res = spark.sql(sqlString)

res.show(res.count(), False)
t2 = time()
print('Time: ' + str(t2-t1))
print('-------------------------------------')
# res.coalesce(1).write.csv("/output/sql/q3_csv.csv")

