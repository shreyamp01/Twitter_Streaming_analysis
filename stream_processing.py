from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, avg, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import mysql.connector
from mysql.connector import Error

# ================================
# Initialize MySQL Connection and Create Tables
# ================================
def init_mysql():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Sairam@123",
            database="twitter_analysis"
        )
        
        cursor = connection.cursor()
        
        # Create tables if they don't exist
        tables = {
            "raw_tweets": """
                CREATE TABLE IF NOT EXISTS raw_tweets (
                    id_str VARCHAR(20) PRIMARY KEY,
                    text TEXT,
                    created_at VARCHAR(50),
                    hashtags VARCHAR(255),
                    user_id VARCHAR(20),
                    user_name VARCHAR(50),
                    retweet_count INT,
                    favorite_count INT,
                    lang VARCHAR(10)
                )
            """,
            "processed_tweets": """
                CREATE TABLE IF NOT EXISTS processed_tweets (
                    tweet_id VARCHAR(20) PRIMARY KEY,
                    text TEXT,
                    user VARCHAR(50),
                    hashtags VARCHAR(255),
                    timestamp VARCHAR(50),
                    retweets INT,
                    likes INT
                )
            """,
            "analytics_data": """
                CREATE TABLE IF NOT EXISTS analytics_data (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    timestamp VARCHAR(50),
                    user_id VARCHAR(20),
                    username VARCHAR(50),
                    followers INT,
                    engagement INT,
                    language VARCHAR(10),
                    location VARCHAR(100)
                )
            """
        }
        
        for table_name, create_statement in tables.items():
            cursor.execute(create_statement)
            print(f"✅ Table {table_name} ready")
            
        connection.commit()
        print("Database initialized successfully!")
        
    except Error as e:
        print(f"Error initializing MySQL: {e}")
        raise e
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Initialize MySQL tables
init_mysql()

# ================================
# Initialize Spark Session
# ================================
spark = SparkSession.builder \
    .appName("TwitterStreamProcessor") \
    .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.28") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ================================
# Define Updated Schemas
# ================================
raw_schema = StructType([
    StructField("id_str", StringType()),
    StructField("text", StringType()),
    StructField("created_at", StringType()),
    StructField("hashtags", StringType()),
    StructField("user_id", StringType()),
    StructField("user_name", StringType()),
    StructField("retweet_count", IntegerType()),
    StructField("favorite_count", IntegerType()),
    StructField("lang", StringType())
])

processed_schema = StructType([
    StructField("tweet_id", StringType()),
    StructField("text", StringType()),
    StructField("user", StringType()),
    StructField("hashtags", StringType()),
    StructField("timestamp", StringType()),
    StructField("retweets", IntegerType()),
    StructField("likes", IntegerType())
])

analytics_schema = StructType([
    StructField("timestamp", StringType()),
    StructField("user_id", StringType()),
    StructField("username", StringType()),
    StructField("followers", IntegerType()),
    StructField("engagement", IntegerType()),
    StructField("language", StringType()),
    StructField("location", StringType())
])

# ================================
# Store to MySQL with Error Handling
# ================================
def store_to_mysql(df, batch_id, table_name):
    try:
        print(f"\n=== Batch {batch_id} → Storing into `{table_name}` ===")
        df.show(5, truncate=False)
        
        # Configure MySQL properties
        mysql_properties = {
            "driver": "com.mysql.cj.jdbc.Driver",
            "url": "jdbc:mysql://localhost:3306/twitter_analysis",
            "user": "root",
            "password": "Sairam@123"
        }
        
        # Write with error handling
        df.write \
            .format("jdbc") \
            .options(**mysql_properties) \
            .option("dbtable", table_name) \
            .mode("append") \
            .save()
            
        print(f"✅ Successfully stored batch {batch_id} to {table_name}")
        
    except Exception as e:
        print(f"❌ Error storing batch {batch_id} to {table_name}: {e}")
        # Log the error but don't raise to continue processing

# ================================
# RAW Tweets Processing
# ================================
def raw_tweets_analysis(df, batch_id):
    store_to_mysql(df, batch_id, "raw_tweets")

# ================================
# PROCESSED Tweets Processing
# ================================
def processed_tweets_analysis(df, batch_id):
    store_to_mysql(df, batch_id, "processed_tweets")

    print("\n=== User Tweet Counts ===")
    df.groupBy("user").agg(count("*").alias("tweet_count")).orderBy(col("tweet_count").desc()).show()

    print("\n=== Average Metrics ===")
    df.select(
        avg(col("retweets")).alias("avg_retweets"),
        avg(col("likes")).alias("avg_likes")
    ).show()

    print("\n=== Hashtag Popularity ===")
    df.groupBy("hashtags").agg(count("*").alias("count")).orderBy(col("count").desc()).show()

# ================================
# ANALYTICS Tweets Processing
# ================================
def analytics_tweets_analysis(df, batch_id):
    store_to_mysql(df, batch_id, "analytics_data")

    print("\n=== Engagement by Language ===")
    df.groupBy("language").agg(
        avg("engagement").alias("avg_engagement"),
        sum("engagement").alias("total_engagement")
    ).show()

    print("\n=== Top Locations by Followers ===")
    df.groupBy("location").agg(
        sum("followers").alias("total_followers")
    ).orderBy(col("total_followers").desc()).show()

    print("\n=== Top Users by Engagement ===")
    df.groupBy("username").agg(
        sum("engagement").alias("total_engagement")
    ).orderBy(col("total_engagement").desc()).show()

# ================================
# Read Kafka Streams
# ================================
def read_kafka(topic, schema):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

raw_df = read_kafka("raw_tweets", raw_schema)
processed_df = read_kafka("processed_tweets", processed_schema)
analytics_df = read_kafka("analytics", analytics_schema)

# ================================
# Start Streaming Queries
# ================================
print("🚀 Starting Streaming Queries...\n")

raw_query = raw_df.writeStream \
    .outputMode("append") \
    .foreachBatch(raw_tweets_analysis) \
    .start()

processed_query = processed_df.writeStream \
    .outputMode("append") \
    .foreachBatch(processed_tweets_analysis) \
    .start()

analytics_query = analytics_df.writeStream \
    .outputMode("append") \
    .foreachBatch(analytics_tweets_analysis) \
    .start()

# ================================
# Await Termination
# ================================
raw_query.awaitTermination()
processed_query.awaitTermination()
analytics_query.awaitTermination()
