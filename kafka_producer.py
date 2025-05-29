import json
import time
from kafka import KafkaProducer
from datetime import datetime

def create_producer():
    """Create and return a Kafka producer with proper configuration"""
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        key_serializer=str.encode,
        acks='all',  # Ensure all replicas acknowledge
        retries=3,   # Retry failed sends
        max_in_flight_requests_per_connection=1  # Ensure ordered delivery
    )

def send_to_kafka(producer, topic, key, data):
    """Send data to specified Kafka topic with a key and ensure delivery"""
    try:
        future = producer.send(topic, key=key, value=data)
        # Wait for the message to be delivered
        future.get(timeout=10)
        print(f"📤 Sent to {topic} (key: {key}):")
        print(json.dumps(data, indent=2))
    except Exception as e:
        print(f"Error sending to {topic}: {e}")
        raise e

def process_tweet_file(file_path):
    """Process tweets from file and send to Kafka exactly once"""
    producer = None
    try:
        producer = create_producer()
        print(f"🌪️ Starting Twitter data processor...")
        print(f"Processing file: {file_path}\n")
        
        with open(file_path, 'r') as f:
            tweets = json.load(f)
        
        topics = ["raw_tweets", "processed_tweets", "analytics"]
        total_tweets = len(tweets)
        processed_count = 0
        
        for tweet in tweets:
            tweet_id = tweet["id_str"]
            user = tweet["user"]
            
            # 🔵 RAW tweet (flattened user)
            raw = {
                "id_str": tweet["id_str"],
                "text": tweet["text"],
                "created_at": tweet["created_at"],
                "hashtags": tweet["hashtags"][0] if tweet["hashtags"] else "NoHashtag",
                "user_id": user["id_str"],
                "user_name": user["screen_name"],
                "retweet_count": tweet["retweet_count"],
                "favorite_count": tweet["favorite_count"],
                "lang": tweet["lang"]
            }
            send_to_kafka(producer, topics[0], f"raw_{tweet_id}", raw)
            
            # 🟡 PROCESSED tweet (flattened metrics)
            processed = {
                "tweet_id": tweet_id,
                "text": tweet["text"],
                "user": user["screen_name"],
                "hashtags": tweet["hashtags"][0] if tweet["hashtags"] else "NoHashtag",
                "timestamp": tweet["created_at"],
                "retweets": tweet["retweet_count"],
                "likes": tweet["favorite_count"]
            }
            send_to_kafka(producer, topics[1], f"proc_{tweet_id}", processed)
            
            # 🟢 ANALYTICS
            analytics = {
                "timestamp": tweet["created_at"],
                "user_id": user["id_str"],
                "username": user["screen_name"],
                "followers": user["followers_count"],
                "engagement": tweet["retweet_count"] + tweet["favorite_count"],
                "language": tweet["lang"],
                "location": user["location"] if user["location"] else "Unknown"
            }
            send_to_kafka(producer, topics[2], f"analytics_{tweet_id}", analytics)
            
            processed_count += 1
            print(f"Progress: {processed_count}/{total_tweets} tweets processed")
            
    except Exception as e:
        print(f"Error processing tweet file: {e}")
        raise e
    finally:
        if producer:
            # Ensure all messages are sent before closing
            producer.flush(timeout=30)
            producer.close()
            print(f"\n✅ Successfully processed {processed_count} tweets")
            print("✅ Kafka producer closed")

def main():
    file_path = "twitter_data/synthetic_tweets_20250420_125202.json"
    process_tweet_file(file_path)

if __name__ == "__main__":
    main()