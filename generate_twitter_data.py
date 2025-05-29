import json
import random
from datetime import datetime, timedelta
from faker import Faker
import os

fake = Faker()

class TwitterDataGenerator:
    def __init__(self):
        self.data_dir = "twitter_data"
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
        
        # Common hashtags for our synthetic data
        self.hashtags = [
            "#DataScience", "#AI", "#MachineLearning", "#Python", 
            "#BigData", "#Analytics", "#DeepLearning", "#NLP",
            "#DataEngineering", "#CloudComputing", "#IoT", "#Blockchain"
        ]
        
        # Common locations
        self.locations = [
            "New York, USA", "London, UK", "San Francisco, USA",
            "Bangalore, India", "Berlin, Germany", "Tokyo, Japan",
            "Toronto, Canada", "Sydney, Australia", "Paris, France",
            "Singapore", "Dubai, UAE", "Mumbai, India"
        ]
        
        # Common languages
        self.languages = ["en", "es", "fr", "de", "hi", "ja", "ko", "zh"]
        
        # Generate a pool of synthetic users
        self.users = []
        for _ in range(100):
            self.users.append({
                "id_str": str(fake.unique.random_number(digits=18)),
                "screen_name": fake.user_name(),
                "name": fake.name(),
                "followers_count": random.randint(100, 100000),
                "location": random.choice(self.locations)
            })

    def generate_tweet(self):
        """Generate a single synthetic tweet"""
        user = random.choice(self.users)
        tweet_hashtags = random.sample(self.hashtags, random.randint(1, 3))
        
        # Generate tweet text with mentions and hashtags
        mentions = [f"@{fake.user_name()}" for _ in range(random.randint(0, 2))]
        text = fake.sentence() + " " + " ".join(mentions) + " " + " ".join(tweet_hashtags)
        
        # Generate engagement metrics
        retweet_count = random.randint(0, 1000)
        favorite_count = random.randint(0, 2000)
        
        return {
            "id_str": str(fake.unique.random_number(digits=18)),
            "text": text,
            "created_at": (datetime.now() - timedelta(days=random.randint(0, 7))).strftime('%a %b %d %H:%M:%S +0000 %Y'),
            "hashtags": tweet_hashtags,
            "user": user,
            "retweet_count": retweet_count,
            "favorite_count": favorite_count,
            "lang": random.choice(self.languages)
        }

    def generate_dataset(self, count=1000):
        """Generate a dataset of synthetic tweets"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.data_dir}/synthetic_tweets_{timestamp}.json"
            
            print(f"Generating {count} synthetic tweets...")
            tweets_data = []
            
            for i in range(count):
                tweet = self.generate_tweet()
                tweets_data.append(tweet)
                
                if (i + 1) % 100 == 0:
                    print(f"Generated {i + 1} tweets...")
            
            # Save to file
            with open(filename, 'w') as f:
                json.dump(tweets_data, f, indent=2)
            
            print(f"\n✅ Successfully generated {len(tweets_data)} synthetic tweets")
            print(f"Data saved to: {filename}")
            return filename
            
        except Exception as e:
            print(f"Error generating dataset: {e}")
            return None

def main():
    # Initialize generator
    generator = TwitterDataGenerator()
    
    # Generate dataset
    data_file = generator.generate_dataset(count=1000)
    
    if data_file:
        print("\nData generation completed successfully!")
        print(f"Use this file with kafka_producer.py: {data_file}")
    else:
        print("\nData generation failed!")

if __name__ == "__main__":
    main() 