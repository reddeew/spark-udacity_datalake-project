import pyspark
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import (year, month, dayofmonth, hour, weekofyear,monotonically_increasing_id)
from datetime import datetime
import os
from pyspark.sql.functions import dayofweek

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def spark_session_creation():
    spark = SparkSession.builder \
     .appName("PythonSparkJob") \
     .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):

   """ 
    process song data 
	input_Data : reads files from s3 bucket
	ouput_Data : wrotes processed file to s3 bucket
	spark : spark session """
	
    song_data = input_data + 'song_data/A/A/*/*.json'

   # reading song data
    df = spark.read.json(song_data)

   #selecting columns for song table 
    songs_table =df.select('song_id','title','artist_id','year','duration').dropDuplicates()

   # writting song table to s3
    songs_table.write.partitionBy('year','artist_id').parquet(os.path.join(output_data,'songs/songs.parquet'),'overwrite')

   # extracting artist table columns
    artists_table=df.select('artist_id','artist_name','artist_location','artist_latitude','artist_longitude')\
    .withColumnRenamed('artist_name', 'name') \
    .withColumnRenamed('artist_location','location') \
    .withColumnRenamed('aritist_latitude','latitude') \
    .withColumnRenamed('artist_longtitude','longtitude') \
    .dropDuplicates()
  # writting artist table to s3
    artists_table.write.parquet(os.path.join(output_data,'artists/artists.parquet'),'overwrite')

def process_log_data(spark, input_data, output_data):
    """
	log data process funtion 
	input_Data : reads files from s3 bucket
	ouput_Data : wrotes processed file to s3 bucket
	spark : spark session """
	
 
    log_data = input_data + 'log_data/*/*/*.json'

   # reading log data 
    df = spark.read.json(log_data)
    
    # filter table based on actions
    actions_df = df.filter(df.page=='NextSong') \
        .select('ts','userId','level','song','artist','sessionId','location','userAgent')

    # select user table columns  
    users_table = df.select('userid', 'firstName', 'lastName', 'gender', 'level').dropDuplicates()
     # create user  table view 
    users_table.createOrReplaceTempView('users')
     # write data to user parquet file
    artists_table=users_table.write.parquet(os.path.join(output_data,'users/users.parquet'),'overwrite')
    # chagne timestamp
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    actions_df = actions_df.withColumn('timestamp,get_timestamp(actions_df.ts)')
    
    # change datatime
    get_datetime = udf(lambda x : str(datetime.fromtimestamp(int(x)/1000)))
    actions_df = actions_df.withColumn('datetime ,get_datetime(actions_df.ts)')
    
    # select time table column 
    time_table = actions_df.select('datetime') \
                 .withColumn('start_time' , actions_df.datetime) \
                 .withColumn('hour', hour('datetime')) \
                 .withColumn('day', dayofmonth('datetime')) \
                 .withColumn('week', weekofyear('datetime')) \
                 .withColumn('month', month('datetime')) \
                 .withColumn('year', year('datetime')) \
                 .withColumn('weekday', dayofweek('datetime')) \
                 .dropDuplicates()
                 
    
   # write data to table columns
    time_table.write.partitionBy('year','month') \
                 .parquet(os.path.json(output_data,'time/time.parquet'), 'overwrite')
				
   
    song_df = spark.read.json(input_data + 'song-data/A/A/*/*.json')

 
    actions_df= actions_df.alias('log_df')
    song_df = song_df.alias('song_df')
    joined_df = actions_df.join(song_df,col('log_df.artist') ==col('song_df.artist_name'),'inner')
    # select songplay table columns
    songplays_table = joined_df.select(col('log_df.datetime').alias('start_time'),
                                        col('log_df.userId').alias('user_Id'),
                                        col('log_df.level').alias('level'),
                                        col('song_df.song_id').alias('song_id'),
                                        col('song_df.artist_id').alias('artist_id'),
                                        col('log_df.sessionId').alias('session_id'),
                                        col('log_df.location').alias('location'), 
                                        col('log_df.userAgent').alias('user_agent'),
                                        year('log_df.datetime').alias('year'),
                                        month('log_df.datetime').alias('month'))\
                                        .withColumn('songplay_id',monotonically_increasing_id())
    songplays_table.createOrReplaceTempView('songplays')

    # write data to songplays parquet file
    time_table = time_table.alias('timetable')
    songplays_table.write.partitionBy('year','month').parquet(os.path.join(output_data,'songplays/songplays.parquet'),'overwrite')


def main():
    input_data='s3a://udacity-dend/'
    output_data='s3a://udacityeshwarspark/'
udacityeshwarspark
    spark = spark_session_creation()
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)
    #create_spark_session()

if __name__ == "__main__":
    main()
