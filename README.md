# moviedataanalysis

## Introduction
This project focuses on creating a collaborative filtering recommendation system using big data analytics and applications, namely HDFS and PySpark. 
The dataset used contains ratings and details of movies. Through aggregations and joins with the Spark SQL module, we explore the dataset, and construct an ALS based 
recommendation system.
## Dataset used
https://grouplens.org/datasets/movielens/20m/
## EDA
We start a spark session (using pyspark) with 6 executors, driver memory of 5 GB and executor memory of 8 GB.
We then read both the datasets (ratings.csv and movies.csv). We remove the timestamp column from the ratings dataframe as it's unnecessary. The ratings dataframe has userId, 
movieId and rating as attributes. The ‘movies’ dataframe has movieId, title and genres.

![image](https://user-images.githubusercontent.com/57229722/106375158-caeeb400-63af-11eb-851b-26726b3c32fb.png)

We then join the ratings and movies dataframe as it would be more convenient for us to group movies together for analysis. We do this on the common attribute ‘movieId’. As seen 
below, for every user you can see the title and genre of each movie watched. 

![image](https://user-images.githubusercontent.com/57229722/106375166-e0fc7480-63af-11eb-95cf-49f8251bb4d0.png)

The 10 movies with the most ratings were displayed as shown. We grouped the movies by movieId, counted the number of ratings for each movie and sorted it(descending order).

![image](https://user-images.githubusercontent.com/57229722/106375185-0a1d0500-63b0-11eb-8b28-f188f476647c.png)

Now, we calculate the average rating for each movie  and sort it. We end up with the top rated movies in the dataset.

![image](https://user-images.githubusercontent.com/57229722/106375199-24ef7980-63b0-11eb-81d6-f5d5302a37bf.png)

The issue with the above screenshot is that it ends up displaying movies with one rating, which does not factor in popularity. So, we try sorting the number of ratings in 
descending order.

![image](https://user-images.githubusercontent.com/57229722/106375213-4cdedd00-63b0-11eb-887f-a4ed8d4a5286.png)

People generally don't collectively agree on rating a movie a 5/5, so it is nearly impossible for even somewhat well known movies to have an average rating of 5 as even a 4.9 
would bring it down from 5. To get a better picture we summarize the dataset and find out that most movies have less than 205 ratings (75th percentile).

![image](https://user-images.githubusercontent.com/57229722/106375232-87e11080-63b0-11eb-9267-7466938fbe1d.png)

![image](https://user-images.githubusercontent.com/57229722/106375242-98918680-63b0-11eb-839c-833ac6b11b16.png)

So, we select the highest rated movies with at least 400 ratings. By doing this, we would have movies with ratings higher than 4 (not 5, but this makes more sense). The 
constraint we have taken is based on the distribution of the dataset.

![image](https://user-images.githubusercontent.com/57229722/106375271-c1198080-63b0-11eb-8b37-7f750af2f116.png)

## Recommendation system
Loading ratings.csv and movies.csv:

![image](https://user-images.githubusercontent.com/57229722/106375286-ec03d480-63b0-11eb-9bb1-1aa3611f5610.png)

![image](https://user-images.githubusercontent.com/57229722/106375294-fd4ce100-63b0-11eb-80aa-e02fd46575ce.png)

Joining the datasets:

![image](https://user-images.githubusercontent.com/57229722/106375301-13f33800-63b1-11eb-8ad5-b0e7f3190429.png)

We drop the column "genres":

![image](https://user-images.githubusercontent.com/57229722/106375317-384f1480-63b1-11eb-8999-48c1107df276.png)

Feature engineering:

![image](https://user-images.githubusercontent.com/57229722/106375350-5ddc1e00-63b1-11eb-9000-b880aa7b6337.png)

The data is split into training (70%) and testing (30%).
ALS algorithm on the training model:

![image](https://user-images.githubusercontent.com/57229722/106375387-8bc16280-63b1-11eb-92c8-51000ffa8a51.png)

Evaluation:

![image](https://user-images.githubusercontent.com/57229722/106375407-b8757a00-63b1-11eb-84e4-73428711eb4c.png)
The RMSE for the test data was 0.813714

Recommendations:

![image](https://user-images.githubusercontent.com/57229722/106375415-d7740c00-63b1-11eb-816e-06b191399826.png)

Movies not watched:

![image](https://user-images.githubusercontent.com/57229722/106375432-f5417100-63b1-11eb-8f3d-03c81845e2ec.png)

Personalized recommendations:

![image](https://user-images.githubusercontent.com/57229722/106375444-08ecd780-63b2-11eb-87d1-ae581e1f3cc7.png)
