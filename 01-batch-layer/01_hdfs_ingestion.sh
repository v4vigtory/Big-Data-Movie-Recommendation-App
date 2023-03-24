scp -i ~/.ssh/vigneshv.pem vigneshv_movies.dat hadoop@ec2-54-166-56-39.compute-1.amazonaws.com:vigneshv
scp -i ~/.ssh/vigneshv.pem vigneshv_ratings.dat hadoop@ec2-54-166-56-39.compute-1.amazonaws.com:vigneshv
scp -i ~/.ssh/vigneshv.pem vigneshv_users.dat hadoop@ec2-54-166-56-39.compute-1.amazonaws.com:vigneshv

hdfs dfs -mkdir /vigneshv/movies
hdfs dfs -mkdir /vigneshv/ratings
hdfs dfs -mkdir /vigneshv/users

hdfs dfs -put vigneshv_movies.dat /vigneshv
hdfs dfs -put vigneshv_ratings.dat /vigneshv
hdfs dfs -put vigneshv_users.dat /vigneshv