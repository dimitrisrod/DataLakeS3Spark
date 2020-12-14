import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
            
    """
    Create the Spark session needed for the ETL process
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
        
    """
    Read Songs and Artists from S3 bucket JSON files, 
    create views for the SparkSQL queries 
    and generate parkquet files on a different S3 bucket with table data (songs and artists) based on the queries.
    """
    
    # get filepath to song data file
    song_data = input_data + 'song_data/A/A/A/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    
    # create temp view for Spark SQL queries
    df.createOrReplaceTempView("songs_data")

    # extract columns to create songs table
    songs_table = spark.sql("""
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM songs_data WHERE song_id IS NOT NULL""")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + 'songs_table/')

    # extract columns to create artists table
    artists_table = spark.sql("""
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM songs_data WHERE artist_id IS NOT NULL""") 
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists_table/')


def process_log_data(spark, input_data, output_data):
    
    """
    Read log data from S3 bucket JSON files, 
    create views for the SparkSQL queries 
    and generate parkquet files on a different S3 bucket with table data (users, time, songplays) based on the queries.

    """
        
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # create temp view for Spark SQL queries
    df.createOrReplaceTempView("logs_data")
    
    # extract columns for users table    
    users_table = spark.sql("""
    SELECT DISTINCT userId, firstName, lastName, gender, level
    FROM logs_data WHERE page = 'NextSong' AND userId IS NOT NULL""")
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users_table/')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x) / 1000)))
    df = df.withColumn('timestamp', get_timestamp(df['ts']))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn('datetime', get_datetime(df['ts']))
    
    # create temp view for SQL queries
    df.createOrReplaceTempView("time_data")
    
    # extract columns to create time table
    time_table = spark.sql("""
    SELECT DISTINCT datetime as start_time,
        hour(datetime) AS hour,
        day(datetime) AS day,
        weekofyear(datetime) AS week,
        month(datetime) AS month,
        year(datetime) AS year,
        dayofweek(datetime) AS weekday
    FROM time_data""")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + 'time_table/')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/A/A/A/*.json')
    
        # create temp view for Spark SQL queries
    song_df.createOrReplaceTempView("songs_data")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
    SELECT monotonically_increasing_id() AS songplay_id,
        to_timestamp(logs_data.ts/1000) AS start_time,
        month(to_timestamp(logs_data.ts/1000)) AS month,
        year(to_timestamp(logs_data.ts/1000)) AS year,
        logs_data.userId AS user_id,
        logs_data.level AS level,
        songs_data.song_id AS song_id,
        songs_data.artist_id AS artist_id,
        logs_data.sessionId AS session_id,
        logs_data.location AS location,
        logs_data.userAgent AS user_agent
    FROM logs_data JOIN songs_data ON logs_data.artist = songs_data.artist_name""")
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + 'songplays_table/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dimitrios-roditis-udacity/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
