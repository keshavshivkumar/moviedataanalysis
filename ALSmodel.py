from pyspark.sql import SparkSession
df = spark.read.csv('hdfs://localhost:9000/input/ratings.csv', inferSchema=True, header=True)
df = df.drop('Timestamp')
df.show(10)
df1 = spark.read.csv('hdfs://localhost:9000/input/movies.csv', inferSchema=True, header=True)
df1.show(10)
df2 = df.join(df1, df.movieId == df1.movieId)
df2.show(10)
df2 = df2.drop('genres','movieId')
df2.show(25)
print((df.count(), len(df.columns)))
print((df1.count(), len(df1.columns)))
print((df2.count(), len(df2.columns)))
df2.printSchema()
userCount_df = df2.groupBy('userId').count().orderBy('count', ascending=False)
user_df = userCount_df.toPandas()
user_df.head() 
user_df.tail()
from pyspark.sql.functions import *
from pyspark.ml.feature import StringIndexer, IndexToString
stringIndexer = StringIndexer(inputCol='title', outputCol='title_num')
model = stringIndexer.fit(df2)
new_df  = model.transform(df2) 
new_df.show(10)
new_df.groupBy('title_num').count().orderBy('count', ascending=False).show(10)
train_df, test_df = new_df.randomSplit([0.7, 0.3])
train_df.show(5)
from pyspark.ml.recommendation import ALS
rec = ALS(maxIter=10, regParam=0.01, userCol='userId', itemCol='title_num', ratingCol='rating', nonnegative=True,coldStartStrategy='drop')
rs_model = rec.fit(train_df)
test_df.show(4) 
test = test_df.select(['userId','title','title_num'])
test_pred = rs_model.transform(test)
test_pred.show()
from pyspark.ml.evaluation import RegressionEvaluator
test_pred = rs_model.transform(test_df)
evaluate_result = RegressionEvaluator(metricName='rmse', predictionCol='prediction', labelCol='rating')
rmse = evaluate_result.evaluate(test_pred)
print('test rmse is %f'%rmse)
nunique_movies = new_df.select('title').distinct()
nunique_movies.count()
a = nunique_movies.alias('a')
user_id = 66
watched_movies = new_df.filter(new_df['userId'] == user_id).select('title').distinct()
b = watched_movies.alias('b')
total_movies = a.join(b, a.title == b.title, how='left')
total_movies.show(25)
user_66_not_watched_movies = total_movies.where(col('b.title').isNull()).select(a.title).distinct()
print('user 66 has not seen the movie %d'%user_66_not_watched_movies.count())
user_66_not_watched_movies = user_66_not_watched_movies.withColumn('userId', lit(int(user_id)))
user_66_not_watched_movies.show(10, False)
user_66_df = model.transform(user_66_not_watched_movies)
user_66_rs = rs_model.transform(user_66_df).orderBy('prediction', ascending=False)
user_66_rs.show(10)





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





