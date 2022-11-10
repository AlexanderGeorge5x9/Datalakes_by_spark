import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
     initialize SparkSession
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Reads and Pprocesses song data in JSON format from S3 bucket. 
    Argumemnts:
        - spark: supplied SparkSession
        - input_data: Input S3 bucket to read from
        - output_data: Output S3 bucket to write to
    Returns:
        Create two folders in S3 for Songs table and Artist table.
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/A/A/A/*.json')
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("songs_data")

    # extract columns to create songs table
    songs_table = spark.sql("""SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration
    FROM songs_data
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data, "songs"), mode='overwrite')

    # extract columns to create artists table
    artists_table = spark.sql("""SELECT DISTINCT
    artist_id,
    artist_name,
    artist_location,
    artist_lattitude,
    artist_longitude
    FROM songs_data
    """)
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, "artist"), mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Reads and Processes log data in JSON format from S3 bucket. 
    Argumemnts:
        - spark: supplied SparkSession
        - input_data: Input S3 bucket to read from
        - output_data: Output S3 bucket to write to
    Returns:
     Create tree folders in S3 for users table, time table and songplays table.
    """
    
    # get filepath to log data file
    log_data =os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.load(log_data).toPandas()
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    df.createOrReplaceTempView("log_data")

    # extract columns for users table    
    users_table = spark.sql("""SELECT DISTINNCT 
    user_id,
    first_name,
    last_name,
    gender,
    level
    FROM log_data
    """)
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, "users"), mode='overwrite')
    # extract columns to create time table
    time_table = spark.sql("""SELECT ts,
    FROM_UNIXTIME(ts/1000, "hh:mm:ss") AS start_time,
    FROM_UNIXTIME(ts/1000, "yyyy-MM-dd") AS date,
    MONTH(FROM_UNIXTIME(ts/1000, "yyyy-MM-dd")) AS month,
    YEAR(FROM_UNIXTIME(ts/1000, "yyyy-MM-dd")) AS year,
    DAY(FROM_UNIXTIME(ts/1000, "yyyy-MM-dd")) AS day,
    WEEKOFYEAR(FROM_UNIXTIME(ts/1000, "yyyy-MM-dd")) AS week,
    HOUR(FROM_UNIXTIME(ts/1000, "hh:mm:ss")) AS hour,
    WEEKDAY(FROM_UNIXTIME(ts/1000, "yyyy-MM-dd")) AS weekday
    FROM log_data
    """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "time"), mode='overwrite')
    # read in song data to use for songplays table
    song_data = os.path.join(input_data, 'song_data/A/A/A/*.json')
    song_df = spark.read.json(song_data)
    song_df.createOrReplaceTempView("songs_data")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql("""SELECT DISTINCT
    FROM_UNIXTIME(LO.ts/1000, "hh:mm:ss") AS start_time,
    YEEAR(FROM_UNIXTIME(LO.ts/1000, "yyyy-MM-dd")) AS year,
    MONTH(FROM_UNIXTIME(LO.ts/1000, "yyyy-MM-dd")) AS month,
    CAST(LO.userid as int) as id,
    LO.level,
    SO.song_id,
    SO.artist_id,
    LO.sessionid,
    LO.location,
    LO.useragent
    FROM log_data LO
    LEFT JOIN songs_data SO
    ON LO.artist = son.artist_name
    AND LO.length = SO.duration
    AND LO.song = SO.title
    WHERE LO.userid != ''
    AND LO.page = 'NextSong'
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "songplays"), mode='overwrite')

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://sparkpro/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
