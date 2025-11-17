from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from dotenv import load_dotenv
import os
from jobs.urban-city import secret_key

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(BASE_DIR, ".env")

# Load .env from the project root
load_dotenv(dotenv_path)

access_key = os.getenv("AWS_ACCESS_KEY_ID")
secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

def main():
    spark = SparkSession.builder.appName("UrbanCityStreaming")\
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1",
                "org.apache.hadoop:hadoop-aws:3.4.1",
                "com.amazonaws:aws-java-sdk:1.12.779")\
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
        .getOrCreate()
        
    # Log level
    spark.sparkcontext.setLogLevel("WARN")
    
    # Vehicle data schema
    vehicle_schema = StructType([
        
    ])

if __name__ == "__main__":
    spark = SparkSession.builder