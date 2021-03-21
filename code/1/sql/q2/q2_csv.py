from pyspark.sql import SparkSession
from time import time

spark = SparkSession.builder.appName("query2-sql").getOrCreate()

t1 = time()
ratings = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/movie_data/ratings.csv")

ratings.registerTempTable("ratings")

sqlString = \
    "select Count*100/Total as Percentage " +\
    "from ( " +\
        "select count(*) as Count, ( " +\
            "select count(distinct(rt._c0)) " +\
            "from ratings as rt) as Total "  +\
        "from ( " +\
            "select r._c0 as User " +\
            "from ratings as r "  +\
            "group by r._c0 "  +\
            "having avg(r._c2) > 3) as u "  +\
    ") as c"
        
res = spark.sql(sqlString)

res.show(res.count(), False)
t2 = time()
print('Time: ' + str(t2-t1))
print('-------------------------------------')
#res.coalesce(1).write.csv("/output/sql/q2_csv.csv")
