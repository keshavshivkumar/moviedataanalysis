from pyspark.sql import SparkSession
df = spark.read.csv('hdfs://localhost:9000/input/ratings.csv', inferSchema=True, header=True)
df = df.drop('Timestamp')
df.show(10)
df1 = spark.read.csv('hdfs://localhost:9000/input/movies.csv', inferSchema=True, header=True)
df1.show(10)
movieratings = df.join(df1, df.movieId == df1.movieId).drop(df1.movieId)
movieratings.show(10)

from pyspark.sql.functions import *
#Most popular movies
popular = movieratings\
.groupBy("movieId")\
.agg(count("userId"))\
.withColumnRenamed("count(userId)", "No_of_Ratings")\
.sort(desc("No_of_Ratings"))

popular.show(10)

populartitle = movieratings\
.groupBy("title")\
.agg(count("userId"))\
.withColumnRenamed("count(userId)", "No_of_Ratings")\
.sort(desc("No_of_Ratings"))

populartitle.show(10)
#Top rated movies

HighestRating = movieratings\
.groupBy("title","genres","movieId")\
.agg(avg(col("rating")),count("userId"))\
.withColumnRenamed("avg(rating)", "AverageRating")\
.withColumnRenamed("count(userId)", "No_of_Ratings")\
.sort(desc("AverageRating"))

HighestRating.show(10)
HighestRatingPopular = HighestRating.sort(desc("AverageRating"),desc("No_of_Ratings")).show(10)
#summary
HighestRatingPopular.select([mean('No_of_Ratings'), min('No_of_Ratings'), max('No_of_Ratings')]).show(1)
#quantiles
HighestRating.approxQuantile('No_of_Ratings', [0.5], 0)
HighestRating.approxQuantile('No_of_Ratings', [0.25], 0)
HighestRating.approxQuantile('No_of_Ratings', [0.75], 0)
HighestRating.approxQuantile('No_of_Ratings', [0.9], 0)
#Most Popular highly rated movies
HighestRating.where("No_of_Rating > 400").show(20, truncate=False)

PolarisedMovies = movieratings\
.groupBy("title","genres","movieId")\
.agg(count("userId").alias("No_of_Ratings"), 
     avg(col("rating")).alias("AverageRating"),
     stddev(col("rating")).alias("StandardDeviation")
    )\
.where("No_of_Ratings > 400")
