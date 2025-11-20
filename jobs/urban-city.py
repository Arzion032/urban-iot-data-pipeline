from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, IntegerType
from pyspark.sql.functions import from_json, col
from dotenv import load_dotenv
from pyspark.sql import DataFrame
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(BASE_DIR, ".env")

# Load .env from the project root
load_dotenv(dotenv_path)

access_key = os.getenv("AWS_ACCESS_KEY_ID")
secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

def main():
    print(f"DEBUG: dotenv_path = {dotenv_path}")
    print(f"DEBUG: File exists? {os.path.exists(dotenv_path)}")
    print(f"DEBUG: access_key = {access_key}")
    print(f"DEBUG: secret_key = {'*' * len(secret_key) if secret_key else None}")  # Don't print actual secret


    
    spark = SparkSession.builder.appName("UrbanCityStreaming")\
        .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,"
                "org.apache.hadoop:hadoop-aws:3.4.1,"
                "com.amazonaws:aws-java-sdk:1.12.524"
            )\
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
        .getOrCreate()
        
    # Log level
    spark.sparkContext.setLogLevel("WARN")
    
    # Vehicle data schema
    vehicle_schema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),    
        StructField("location", StringType(), True),
        StructField("speed", FloatType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuel_type", StringType(), True),
    ])
    
    # GPS data schema
    gps_schema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),    
        StructField("speed", FloatType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicle_type", StringType(), True),
    ])  
    
    # Traffic camera data schema
    traffic_schema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),    
        StructField("location", StringType(), True),
        StructField("camera_id", StringType(), True),
        StructField("snapshot", StringType(), True),
    ])
    
    # Weather data schema
    weather_schema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),    
        StructField("location", StringType(), True),
        StructField("temperature_celsius", FloatType(), True),
        StructField("humidity_percent", FloatType(), True),
        StructField("condition", StringType(), True),
        StructField("precipitation_mm", FloatType(), True),
        StructField("wind_speed_kph", FloatType(), True),
        StructField("air_quality_index", FloatType(), True),
    ])
    
    # Emergency incident data schema
    emergency_schema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),    
        StructField("type", StringType(), True),
        StructField("location", StringType(), True),
        StructField("severity", IntegerType(), True),
    ])     
    
    def read_kafka_topic(topic, schema):
        return (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "broker:29092")
            .option('subscribe', topic)
            .option("startingOffsets", "earliest") 
            .load()
            .selectExpr('CAST(value AS STRING)')
            .select(from_json(col('value'), schema).alias('data'))
            .select('data.*')
            .withWatermark('timestamp', '2 minutes')
            )
    
    def stream_writer(input: DataFrame, checkpoint_folder, output):
        return(input.writeStream
               .format("parquet")
               .option("checkpointLocation", checkpoint_folder)
               .option("path", output)
               .outputMode("append")
               .start()
               )
        
        
    vehicle_df = read_kafka_topic('vehicle_topic', vehicle_schema).alias("vehicle")
    gps_df = read_kafka_topic('gps_topic', gps_schema).alias("gps")
    traffic_df = read_kafka_topic('traffic_topic', traffic_schema).alias("traffic")
    weather_df = read_kafka_topic('weather_topic', weather_schema).alias("weather")
    emergency_df = read_kafka_topic('emergency_topic', emergency_schema).alias("emergency")

    # Join all the dataframes using id and timestamp
    
    query_1 = stream_writer(vehicle_df, 's3a://urban-spark-streaming-data/checkpoints/vehicle_data',
                  's3a://urban-spark-streaming-data/data/vehicle_data')
    query_2 = stream_writer(gps_df, 's3a://urban-spark-streaming-data/checkpoints/gps_data',
                  's3a://urban-spark-streaming-data/data/gps_data')
    query_3 = stream_writer(traffic_df, 's3a://urban-spark-streaming-data/checkpoints/traffic_data',
                  's3a://urban-spark-streaming-data/data/traffic_data')
    query_4 = stream_writer(weather_df, 's3a://urban-spark-streaming-data/checkpoints/weather_data',
                  's3a://urban-spark-streaming-data/data/weather_data')
    query_5 = stream_writer(emergency_df, 's3a://urban-spark-streaming-data/checkpoints/emergency_data',
                  's3a://urban-spark-streaming-data/data/emergency_data')
    
    query_5.awaitTermination()

if __name__ == "__main__":
    main()