import pyspark
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import (year, month, dayofmonth, hour, weekofyear,monotonically_increasing_id)
from datetime import datetime
import os

config = configparser.ConfigParser()
config.read('aws.cfg')
input_s3 = config['bucket']['input_s3']
output_s3= config['bucket']['output_s3']

#os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def spark_session_creation():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    song_data = input_data + 'song_data/*/*/*/*.json'

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table =df.select('song_id','title','artist_id','year','duration').dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id') .parquet(os.path.join(output_data,'songs/songs.parquet'),'overwrite')

    # extract columns to create artists table
    artists_table =df.select('artist_id','artist_name','artist_location','artist_latitude','artist_longtitude')\
    .withColumnRenamed('artist_name', 'name') \
    .withColumnRenamed('artist_location','location') \
    .wthColumnRenamed('aritist_latitude','latitude') \
    .withColumnRenamed('artist_longtitude','longtitude') \
    .dropDuplicates()
    artists_table.write.parquet(os.path.join(output_data,'artists/artists.parquet'),'overwrite')

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    actions_df = df.filter(df.page=='NextSong') \
        .select('ts','userId','level','song','artist','sessionId','location','userAgent')

    # extract columns for users table    
    users_table = df.select('userid', 'firstName', 'lastName', 'gender', 'level').dropDuplicates()

    users_table.createOrReplaceTempView('users')
    # write users table to parquet files
    artists_table=users_table.write.parquet(os.path.join(output_data,'users/users.parquet'),'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    action_df = actions_df.withColumn('timestamp,gettimestamp(actions_df.ts)')
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x : str(datatime.fromtimestamp(int(x)/1000)))
    action_df = action_df.withColumn('datetime ,get_datetime(actions_df.ts)')
    
    # extract columns to create time table
    time_table = action_df.select('datetime') \
                 .withColumn('start_time' , action_df.datetime) \
                 .withColumn('hour', hour('datetime')) \
                 .withColumn('day', dayofmonth('datetime')) \
                 .withColumn('week', weekofyear('datetime')) \
                 .withColumn('month', month('datetime')) \
                 .withColumn('year', year('datetime')) \
                 .withColumn('weekday', dayofweek('datetime')) \
                 .dropDuplicates()
                 
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month') \
                 .parquet(os.path.json(output_data,'time/time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song-data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table 
    action_df= actions_df.alias('log_df')
    song_df = songdf_df.alias('song_df')
    joined_df = action_df.join(song_df,col('log_df.artist') ==col('song_df.artist_name'),'inner')
    
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
                                        .withColumn('songplay_id,monotonically_increasing_id()')
    songplays_table.createOrReplaceTempView('songplays')

    # write songplays table to parquet files partitioned by year and month
    time_table = time_table.alias('timetable')
    songplays_table.write.partitionBy('year','month').parquet(os.path.join(output_data,'songplays/songplays.parquet'),'overwrite')


def main():
    input_data=input_s3
    output_data=output_s3

    spark = spark_session_creation()
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)
    create_spark_session()

if __name__ == "__main__":
    main()
