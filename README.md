Project: Data Lake
Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

Project Datasets
You'll be working with two datasets that reside in S3. Here are the S3 links for each:

Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data
Song Dataset
The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json

How to run :

Create EMR cluster in AWS
and create Notebook for EMR cluster and run the code from terminal
schema for song play analysis:

reads data from s3 log_data,song_data folders and write as parquet files after processing


user tabl schema - userid, firstName, lastName, gender, level
artist table = artist_id,artist_name,artist_location,artist_latitude,artist_longtude


time  - datatime,start_time,hour,day,weeek,month,year,weekday

songplay - start_time,userid,level,song_id,artist_id,sessionid,location,useragent,datatime
