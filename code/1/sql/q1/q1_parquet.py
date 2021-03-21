from pyspark.sql import SparkSession
from time import time

spark = SparkSession.builder.appName("query1-rdd").getOrCreate()

t1 = time()
movies = spark.read.parquet("hdfs://master:9000/movie_data/movies.parquet")

movies.registerTempTable("movies")

sqlString = \
	"select year(m1._c3) as Year, m1._c1 as Title, ((m1._c6 - m1._c5)*100/m1._c5) as Profit " +\
	"from movies as m1 " +\
	"left outer join movies as m2 " +\
  		"on year(m1._c3) = year(m2._c3) " +\
        	"and (((m1._c6 - m1._c5)*100/m1._c5) < ((m2._c6 - m2._c5)*100/m2._c5) " +\
        	"or (((m1._c6 - m1._c5)*100/m1._c5) = ((m2._c6 - m2._c5)*100/m2._c5) and m1._c0 < m2._c0)) " +\
	"where year(m2._c3) is null and m1._c6 > 0 and ((m1._c6 - m1._c5)*100/m1._c5) is not null and year(m1._c3) >= '2000' " +\
	"order by year(m1._c3)"

res = spark.sql(sqlString)

res.show(res.count(), False)
t2 = time()
print('Time: ' + str(t2-t1))
print('-------------------------------------')
#res.coalesce(1).write.csv("/output/sql/q1_parquet.csv")
