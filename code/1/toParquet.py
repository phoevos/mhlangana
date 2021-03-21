from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("toParquet").getOrCreate()
sc = spark.sparkContext

movies = spark.read.format("csv").option("header", "false").load("hdfs://master:9000/movie_data/movies.csv")
movies.write.parquet("hdfs://master:9000/movie_data/movies.parquet")

movie_genres = spark.read.format("csv").option("header", "false").load("hdfs://master:9000/movie_data/movie_genres.csv")
movie_genres.write.parquet("hdfs://master:9000/movie_data/movie_genres.parquet")

ratings = spark.read.format("csv").option("header", "false").load("hdfs://master:9000/movie_data/ratings.csv")
ratings.write.parquet("hdfs://master:9000/movie_data/ratings.parquet")

df2 = spark.read.parquet("hdfs://master:9000/movie_data/movies.parquet")
df2.show()
