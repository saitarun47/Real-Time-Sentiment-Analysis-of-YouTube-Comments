import requests
import pandas as pd
import time
import firebase_admin
from firebase_admin import credentials, firestore
from transformers import pipeline
import os
from dotenv import load_dotenv



load_dotenv()


firebase_credentials_path = os.getenv("FIREBASE_CREDENTIALS_PATH")


if not firebase_admin._apps:
    cred = credentials.Certificate(firebase_credentials_path)
    firebase_admin.initialize_app(cred)
else:
    app = firebase_admin.get_app()  

# Initialize Firestore
db = firestore.client()

# YouTube API key and Video ID
API_KEY = os.getenv("API_KEY")
VIDEO_ID = os.getenv("VIDEO_ID")

# Initializing the Hugging Face sentiment analysis pipeline 
sentiment_pipeline = pipeline("sentiment-analysis")

# Function to perform sentiment analysis using Hugging Face transformer model
def get_sentiment(comment):
    
    sentiment = sentiment_pipeline(comment)
    sentiment_score = sentiment[0]['score'] if sentiment[0]['label'] == 'POSITIVE' else -sentiment[0]['score']
    
    return sentiment_score

# Function to fetch comments from YouTube API
def fetch_comments():
    
    base_url = f"https://www.googleapis.com/youtube/v3/commentThreads?part=snippet&videoId={VIDEO_ID}&maxResults=1000&key={API_KEY}"
    
    comments_data = []  
    next_page_token = None  

    while True:
        
        url = base_url + (f"&pageToken={next_page_token}" if next_page_token else "")
        response = requests.get(url).json()

        
        if "error" in response:
            print("Error fetching comments:", response['error']['message'])
            break

        # Extract comments from the response
        for video in response.get('items', []):
            if video['kind'] == 'youtube#commentThread':
                video_id = video['snippet']['videoId']
                comment_id = video['id']  
                comment = video['snippet']['topLevelComment']['snippet']['textOriginal']
                comments_data.append({"video_id": video_id, "comment_id": comment_id, "comment": comment})

        
        next_page_token = response.get("nextPageToken")
        if not next_page_token:  
            break

    return pd.DataFrame(comments_data)


# Function to check if a comment is already stored in Firestore
def is_comment_stored(comment_id):
    
    comment_ref = db.collection("comments").where("comment_id", "==", comment_id).get()
    return len(comment_ref) > 0

# Function to upload new comments to Firestore
def upload_to_firestore(df):
    for _, row in df.iterrows():
        if not is_comment_stored(row['comment_id']):
            
            data = {
                'video_id': row['video_id'],
                'comment_id': row['comment_id'],  
                'comment': row['comment'],
                'sentiment_score': row['sentiment_score'],
            }
            db.collection("comments").add(data)
        else:
            print(f"Comment ID {row['comment_id']} already exists, skipping upload.")
    
    print("New comments and sentiment analysis results uploaded successfully!")

# Function to generate a sentiment report for the whole comment section
def generate_sentiment_report(df):
    avg_sentiment_score = df['sentiment_score'].mean()
    
    positive_comments = df[df['sentiment_score'] > 0.1]
    neutral_comments = df[(df['sentiment_score'] <= 0.1) & (df['sentiment_score'] >= -0.1)]
    negative_comments = df[df['sentiment_score'] < -0.1]
    
    sentiment_summary = {
        'average_sentiment_score': avg_sentiment_score,
        'positive_comments_count': len(positive_comments),
        'neutral_comments_count': len(neutral_comments),
        'negative_comments_count': len(negative_comments),
        'total_comments': len(df)
    }
    
    print("Sentiment Report for the Comment Section:")
    print(f"Average Sentiment Score: {avg_sentiment_score:.2f}")
    print(f"Positive Comments: {len(positive_comments)}")
    print(f"Neutral Comments: {len(neutral_comments)}")
    print(f"Negative Comments: {len(negative_comments)}")
    print(f"Total Comments: {len(df)}")
    
    return sentiment_summary

# Main function to fetch comments, perform sentiment analysis, and upload data
def automated_process():
    print("Fetching comments from YouTube...")
    df = fetch_comments()
    
    if df.empty:
        print("No comments found!")
        return None
    
    print("Performing sentiment analysis...")
    df['sentiment_score'] = df['comment'].apply(get_sentiment)
    
    print("Uploading new comments to Firestore...")
    upload_to_firestore(df)
    
    print("Generating sentiment report...")
    sentiment_summary = generate_sentiment_report(df)
    
    return sentiment_summary


if __name__ == "__main__":
    while True:
        sentiment_summary = automated_process()  
        time.sleep(300)  #  5 minutes
