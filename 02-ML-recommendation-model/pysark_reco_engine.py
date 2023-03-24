# Note that when starting up the PySpark shell
# SparkContext is already created and available as 'sc' 
# SparkSession is already created and available as 'spark'

# Below are the explicit commands to create these when running a script
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

conf = SparkConf() #Declare spark conf variable
sc = SparkContext.getOrCreate(conf=conf)

# Instantiate spark builder and Set spark app name. 
# Also, enable hive support using enableHiveSupport option of spark builder.
spark = SparkSession(sc).builder.appName("Recommendation Engine").enableHiveSupport().getOrCreate()

# Read hive table in spark using sql method of SparkSession class
# Hive-style
# vigneshv_ratings = spark.sql("select * from default.vigneshv_ratings")
# vigneshv_movies = spark.sql("select * from default.vigneshv_movies")
# vigneshv_users = spark.sql("select * from default.vigneshv_users")

# Read hive table in spark using table method of SparkSession class
# OO-style
vigneshv_ratings = spark.table("vigneshv_ratings")
vigneshv_movies = spark.table("vigneshv_movies")
vigneshv_users = spark.table("vigneshv_users")
 
# Display the spark dataframe values using show method
vigneshv_ratings.show(10, truncate = False)
vigneshv_movies.show(10, truncate = False)
vigneshv_users.show(10, truncate = False)

# Display the type of DataFrame to confirm
type(vigneshv_ratings)
# <class 'pyspark.sql.dataframe.DataFrame'>

# Building the Recommender Model using ALS
from pyspark.ml.recommendation import ALS

(trainingRatings, testRatings) = vigneshv_ratings.randomSplit([80.0, 20.0])

als = ALS(rank=20, maxIter=10, regParam=0.01, userCol='userid', itemCol='movieid', ratingCol='rating')
model = als.fit(trainingRatings)

from pyspark.sql.functions import lit

# Function to recommend top numReco movies to any user
def recommendMovies(model, user, numReco):
    # Create a Spark DataFrame with the specified user and all the movies listed in the ratings DataFrame    
    dataSet = vigneshv_ratings.select('movieid').distinct().withColumn('userid', lit(user))
    
    # Create a Spark DataFrame with the movies that have already been rated by this user
    moviesAlreadyRated = vigneshv_ratings.filter(vigneshv_ratings.userid == user).select('movieid', 'userid')
    
    # Apply the recommender system to the data set without the already rated movies to predict ratings
    predictions = model.transform(dataSet.subtract(moviesAlreadyRated)).dropna().orderBy('prediction', ascending=False).limit(numReco).select('userid', 'movieid', 'prediction')
    
    # Join with the movies DataFrame to get the movies titles and genres
    recommendations = predictions.join(vigneshv_movies, predictions.movieid == vigneshv_movies.movieid).select(predictions.userid, predictions.movieid, vigneshv_movies.title, vigneshv_movies.genres, predictions.prediction).orderBy('prediction', ascending=False)
    
    return recommendations

import pandas as pd

# Function to create DataFrame with top numReco movie recommendations for ALL users
def AllUsersTopMovieReco(users, numReco=10):
    '''
        users: Spark DataFrame containing the users
    '''
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    # Creating an empty Spark DataFrame to hold all recommendations
    allRecommendations = pd.DataFrame(columns=['userid', 'movieid', 'title', 'genres', 'prediction'])
    users_pandasDF = users.toPandas()
    for _, row in users_pandasDF.iterrows():
        print("Getting recommendations for user: ", row['userid']) 
        newUserPred = recommendMovies(model, row['userid'], numReco)
        allRecommendations = pd.concat([allRecommendations, newUserPred.toPandas()])
    return spark.createDataFrame(allRecommendations)

# Test for Sample users
sample_users = vigneshv_users.limit(10)
SampleUsersTopMovieRecos = AllUsersTopMovieReco(sample_users, numReco=10)

AllUsersTopMovieRecos = AllUsersTopMovieReco(vigneshv_users, numReco=10)

# AllUsersTopMovieRecos.show(50, truncate=False)

# Save the table to Hive
AllUsersTopMovieRecos.write.mode('overwrite').saveAsTable("vigneshv_AllUsersTopMovieRecos")