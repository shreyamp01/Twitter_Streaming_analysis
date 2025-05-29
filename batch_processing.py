from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum, desc, max, current_timestamp
import time

# Initialize Spark Session with MySQL connector
spark = SparkSession.builder \
    .appName("TwitterBatchProcessor") \
    .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.28") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# MySQL connection properties
mysql_properties = {
    "driver": "com.mysql.cj.jdbc.Driver",
    "url": "jdbc:mysql://localhost:3306/twitter_analysis",
    "user": "root",
    "password": "Sairam@123"
}

def read_mysql_table(table_name):
    try:
        start_time = time.time()
        df = spark.read \
            .format("jdbc") \
            .options(**mysql_properties) \
            .option("dbtable", table_name) \
            .load()
        end_time = time.time()
        print(f"✅ Successfully loaded {table_name} in {end_time - start_time:.2f} seconds")
        return df
    except Exception as e:
        print(f"❌ Error loading {table_name}: {e}")
        return None

# Read data from MySQL tables
print("\n=== Loading Data ===")
raw_df = read_mysql_table("raw_tweets")
processed_df = read_mysql_table("processed_tweets")
analytics_df = read_mysql_table("analytics_data")

if raw_df is None or processed_df is None or analytics_df is None:
    print("Failed to load required data from MySQL. Exiting...")
    spark.stop()
    exit(1)

print("\n=== Batch Processing Results ===")

try:
    # Track total processing time
    total_start_time = time.time()
    
    # 1. Basic counts and statistics
    print("\n1. Basic Statistics:")
    start_time = time.time()
    print(f"Total tweets: {raw_df.count()}")
    print(f"Total users: {analytics_df.select('username').distinct().count()}")
    print(f"Total languages: {analytics_df.select('language').distinct().count()}")
    print(f"Total locations: {analytics_df.select('location').distinct().count()}")
    end_time = time.time()
    print(f"Basic statistics processing time: {end_time - start_time:.2f} seconds")

    # 2. User Analysis
    print("\n2. User Analysis:")
    start_time = time.time()
    user_stats = analytics_df.groupBy("username") \
        .agg(
            count("*").alias("tweet_count"),
            sum("followers").alias("total_followers"),
            avg("followers").alias("avg_followers"),
            sum("engagement").alias("total_engagement")
        ) \
        .orderBy(desc("total_engagement"))
    
    user_stats.show(5, truncate=False)
    end_time = time.time()
    print(f"User analysis processing time: {end_time - start_time:.2f} seconds")

    # 3. Tweet Engagement Analysis
    print("\n3. Tweet Engagement Analysis:")
    start_time = time.time()
    engagement_stats = processed_df.select(
        avg("retweets").alias("avg_retweets"),
        max("retweets").alias("max_retweets"),
        avg("likes").alias("avg_likes"),
        max("likes").alias("max_likes")
    )
    engagement_stats.show()
    end_time = time.time()
    print(f"Engagement analysis processing time: {end_time - start_time:.2f} seconds")

    # 4. Language Analysis
    print("\n4. Language Analysis:")
    start_time = time.time()
    language_stats = analytics_df.groupBy("language") \
        .agg(
            count("*").alias("tweet_count"),
            avg("engagement").alias("avg_engagement"),
            sum("engagement").alias("total_engagement")
        ) \
        .orderBy(desc("tweet_count"))
    language_stats.show()
    end_time = time.time()
    print(f"Language analysis processing time: {end_time - start_time:.2f} seconds")

    # 5. Location Analysis
    print("\n5. Location Analysis:")
    start_time = time.time()
    location_stats = analytics_df.groupBy("location") \
        .agg(
            count("*").alias("user_count"),
            sum("followers").alias("total_followers"),
            avg("engagement").alias("avg_engagement")
        ) \
        .orderBy(desc("total_followers"))
    location_stats.show(5, truncate=False)
    end_time = time.time()
    print(f"Location analysis processing time: {end_time - start_time:.2f} seconds")

    # 6. Hashtag Analysis
    print("\n6. Hashtag Analysis:")
    start_time = time.time()
    hashtag_stats = processed_df.groupBy("hashtags") \
        .agg(
            count("*").alias("usage_count"),
            avg("retweets").alias("avg_retweets"),
            avg("likes").alias("avg_likes")
        ) \
        .orderBy(desc("usage_count"))
    hashtag_stats.show(10, truncate=False)
    end_time = time.time()
    print(f"Hashtag analysis processing time: {end_time - start_time:.2f} seconds")

    # 7. Store Aggregated Metrics
    print("\n7. Storing Aggregated Metrics...")
    try:
        start_time = time.time()
        # User Metrics Summary
        user_metrics = user_stats.select(
            col("username"),
            col("tweet_count"),
            col("total_followers"),
            col("avg_followers"),
            col("total_engagement")
        )
        
        user_metrics.write \
            .format("jdbc") \
            .options(**mysql_properties) \
            .option("dbtable", "user_metrics_summary") \
            .mode("overwrite") \
            .save()
        
        # Hashtag Metrics Summary
        hashtag_metrics = hashtag_stats.select(
            col("hashtags"),
            col("usage_count"),
            col("avg_retweets"),
            col("avg_likes")
        )
        
        hashtag_metrics.write \
            .format("jdbc") \
            .options(**mysql_properties) \
            .option("dbtable", "hashtag_metrics_summary") \
            .mode("overwrite") \
            .save()
        
        # Language Metrics Summary
        language_metrics = language_stats.select(
            col("language"),
            col("tweet_count"),
            col("avg_engagement"),
            col("total_engagement")
        )
        
        language_metrics.write \
            .format("jdbc") \
            .options(**mysql_properties) \
            .option("dbtable", "language_metrics_summary") \
            .mode("overwrite") \
            .save()
        
        end_time = time.time()
        print(f"✅ Successfully stored all metrics summaries in {end_time - start_time:.2f} seconds")
        
    except Exception as e:
        print(f"❌ Error storing metrics: {e}")

    total_end_time = time.time()
    print(f"\nTotal batch processing time: {total_end_time - total_start_time:.2f} seconds")

except Exception as e:
    print(f"\n❌ Error during batch processing: {e}")

finally:
    print("\n🔄 Batch processing completed")
    spark.stop()