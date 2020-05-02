import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, to_date, dayofweek, monotonically_increasing_id
from pyspark.sql.types import StructType as R, StructField as Field, DoubleType as Dbl, StringType as Str, IntegerType as Int, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates a spark session.
    
    Parameters:
        None
    
    Returns:
        spark (SparkSession): Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Processes data from the song files and stores the results.
    
    Loads json song files from the S3 bucket that comes in the input_data variable.
    The data is used to create a table for songs and a table for artists.
    The data of the resulting tables is stored in the output_data S3 bucket.
    
    Parameters:
        spark (SparkSession): Spark session
        input_data (String): URL of the S3 bucket with the input files
        output_data (String): URL of the S3 bucket to store the result files
        
    Returns:
        None
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    songSchema = R([
            Field("artist_id",Str()),
            Field("artist_latitude",Dbl()),
            Field("artist_location",Str()),
            Field("artist_longitude",Dbl()),
            Field("artist_name",Str()),
            Field("duration",Dbl()),
            Field("num_songs",Int()),
            Field("title",Str()),
            Field("year",Int()),
        ])

    # read song data file
    df = spark.read.json(song_data, schema=songSchema)

    # extract columns to create songs table
    song_fields = ["title", "artist_id","year", "duration"]
    songs_table = df.select(song_fields).dropDuplicates().withColumn("song_id", monotonically_increasing_id())

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + 'songs/')

    # extract columns to create artists table
    artists_fields = ["artist_id", "artist_name as name", "artist_location as location",
                      "artist_latitude as latitude", "artist_longitude as longitude"]
    artists_table = df.selectExpr(artists_fields).dropDuplicates()

    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/')


def process_log_data(spark, input_data, output_data):
    """
    Processes data from the log files and stores the results.
    
    Loads json log files from the S3 bucket that comes in the input_data variable.
    A time table is created from the timestamp column in the input data.
    The data is filtered and joined with the data in the artist and user tables.
    The joined data is used to create a table for songplays.
    The data of the resulting tables is stored in the output_data S3 bucket.
    
    Parameters:
        spark (SparkSession): Spark session
        input_data (String): URL of the S3 bucket with the input files
        output_data (String): URL of the S3 bucket to store the result files
        
    Returns:
        None
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    user_field_expr = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    users_table = df.selectExpr(user_field_expr).dropDuplicates()

    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/')

    # create datetime column from original timestamp column
    df = df.withColumn('start_time', to_date((df.ts/1000).cast(TimestampType())))

    # extract columns to create time table
    time_table = df.select("start_time").dropDuplicates() \
        .withColumn("hour", hour(col("start_time"))).withColumn("day", dayofmonth(col("start_time"))) \
        .withColumn("week", weekofyear(col("start_time"))).withColumn("month", month(col("start_time"))) \
        .withColumn("year", year(col("start_time"))).withColumn("weekday", dayofweek(col("start_time")))

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + 'time/')

    # read in song and artist data to use for songplays table
    path = output_data + 'songs/*/*/*'
    base_path = output_data + 'songs/'
    songs_df = spark.read.option("basePath", base_path).parquet(path)
    artists_df = spark.read.parquet(output_data + 'artists/*')

    artists_logs = df.join(artists_df, (df.artist == artists_df.name)).drop(artists_df.location)
    artists_songs_logs = artists_logs \
        .join(songs_df, (
                artists_logs.artist_id == songs_df.artist_id),
                how='left'
            ).drop(songs_df.artist_id)

    # extract columns from joined song and log datasets to create songplays table 
    songplays = artists_songs_logs.join(
        time_table,
        artists_songs_logs.start_time == time_table.start_time,
        how='left'
    ).drop(artists_songs_logs.start_time).drop(time_table.year)

    # write songplays table to parquet files partitioned by year and month
    songplays_field_expr = ["start_time", "userId as user_id", "level", "song_id", "artist_id",
                            "sessionId as session_id", "location", "userAgent as user_agent", "year", "month"]
    songplays_table = songplays.selectExpr(songplays_field_expr)
    songplays_table.write.partitionBy("year", "month").parquet(output_data + 'songplays/')


def main():
    """
    Main method for the process.
    
    Input data is loaded from an S3 bucket, transformed and stored in another bucket.
    """
    spark = create_spark_session()
    input_data = "s3a://ud-dend-spark/"
    output_data = "s3a://ud-dend-spark/outputs/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
