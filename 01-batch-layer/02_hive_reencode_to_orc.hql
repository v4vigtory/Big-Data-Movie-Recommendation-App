-- Creating external table for ratings
CREATE EXTERNAL TABLE vigneshv_ratings_dat (
    UserID smallint,
    Blank_First string, 
    MovieID smallint,
    Blank_Second string, 
    Rating decimal,
    Blank_Third string,
    `TimeStamp` int) -- TimeStamp is HIVE keyword, hence enclosed within ``
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ":",
   "quoteChar"     = "\""
)

STORED AS TEXTFILE
  location '/vigneshv/ratings';

-- Note: 
-- Data file is available as a .dat file which uses "::" as a delimiter instead of a single character delimiter such as "," or ":"
-- By default, HIVE only allows user to use single character as field delimiter.
-- So, we are explicitly creating headers for blank columns due to inability to use MultiDelimiterSerde
-- We will drop these blank columns when creating Hive managed Orc tables in the next step

-- Create Hive managed tables as Orc and populating them 
-- with relevant data from the external table

CREATE TABLE vigneshv_ratings(
    UserID smallint,
    MovieID smallint,
    Rating decimal,
    `TimeStamp` int)
    stored as orc;

insert overwrite table vigneshv_ratings 
select UserID, MovieID, Rating, `TimeStamp` from vigneshv_ratings_dat;
-- Note: We are dropping the additional blank columns created

-- select * from vigneshv_ratings limit 10;

-- Creating external table for movies
CREATE EXTERNAL TABLE vigneshv_movies_dat (
    MovieID smallint,
    Blank_First string, 
    Title string,
    Blank_Second string, 
    Genres string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ":",
   "quoteChar"     = "\""
)

STORED AS TEXTFILE
  location '/vigneshv/movies';

-- Create Hive managed tables as Orc and populating them 
-- with relevant data from the external table
CREATE TABLE vigneshv_movies(
    MovieID smallint,
    Title string,
    Genres string)
    stored as orc;

insert overwrite table vigneshv_movies
select MovieID, Title, Genres from vigneshv_movies_dat;

-- select * from vigneshv_movies limit 10;

-- Creating external table for users
CREATE EXTERNAL TABLE vigneshv_users_dat (
    UserID smallint,
    Blank_First string, 
    Gender string,
    Blank_Second string, 
    Age smallint,
    Blank_Third string,
    Occupation string,
    Blank_Fourth string,
    Zipcode int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ":",
   "quoteChar"     = "\""
)

STORED AS TEXTFILE
  location '/vigneshv/users';

-- Create Hive managed tables as Orc and populating them 
-- with relevant data from the external table

CREATE TABLE vigneshv_users(
    UserID smallint,
    Gender string,
    Age smallint,
    Occupation string,
    Zipcode int)
    stored as orc;

insert overwrite table vigneshv_users
select UserID, Gender, Age, Occupation, Zipcode from vigneshv_users_dat;

-- select * from vigneshv_users limit 10;