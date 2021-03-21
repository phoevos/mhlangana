from pyspark.sql import SparkSession
from time import time

def summaryLength(summary):
    words = summary.split()
    return len(words)

def quinquennium(year):
    quint = (int(year)-2000)//5 + 1
    return (str(quint) + "η 5ετία")

spark = SparkSession.builder.appName("query4-sql").getOrCreate()

t1 = time()

spark.udf.register("summaryLength", summaryLength)
spark.udf.register("quinq", quinquennium)

movies = spark.read.parquet("hdfs://master:9000/movie_data/movies.parquet")
movie_genres = spark.read.parquet("hdfs://master:9000/movie_data/movie_genres.parquet")

movies.registerTempTable("movies")
movie_genres.registerTempTable("movie_genres")

sqlString = \
        "select Quinquennium, avg(SummaryLength) as AvgSumLength " +\
        "from ( " +\
            "select quinq(year(m._c3)) as Quinquennium, summaryLength(m._c2) as SummaryLength " +\
            "from movie_genres as g "  +\
            "join movies as m on g._c0 = m._c0 "  +\
            "where g._c1 = 'Drama' and year(m._c3) > '1999' and m._c2 is not null) "  +\
        "group by Quinquennium "  +\
        "order by Quinquennium"

res = spark.sql(sqlString)

res.show(res.count(), False)
t2 = time()
print('Time: ' + str(t2-t1))
print('-------------------------------------')
#res.coalesce(1).write.csv("/output/sql/q4_parquet.csv")

