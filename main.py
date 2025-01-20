import requests
import pandas as pd
import time
import firebase_admin
from firebase_admin import credentials, firestore

# Initialize Firebase
if not firebase_admin._apps:
    cred = credentials.Certificate("C:/Users/tanuj/Downloads/api-7612e-firebase-adminsdk-fbsvc-ee6139b3e3.json")
    firebase_admin.initialize_app(cred)
else:
    app = firebase_admin.get_app()  # Get the already initialized app

# Initialize Firestore
db = firestore.client()

# Define YouTube API key and Video ID
API_KEY = "YOUR_YOUTUBE_API_KEY"
VIDEO_ID = "YOUR_VIDEO_ID"

# Function to fetch comments and upload to Firestore
def fetch_and_upload_comments():
    # Define the URL for fetching comments
    url = f"https://www.googleapis.com/youtube/v3/commentThreads?part=snippet&videoId={VIDEO_ID}&maxResults=50&key={API_KEY}"

    # Fetch response from YouTube API
    response = requests.get(url).json()
    
    # Extract comments and prepare data
    comments_data = []
    for video in response.get('items', []):
        if video['kind'] == 'youtube#commentThread':
            video_id = video['snippet']['videoId']
            comment = video['snippet']['topLevelComment']['snippet']['textOriginal']
            comments_data.append({"video_id": video_id, "comment": comment})

    # Create a DataFrame
    df = pd.DataFrame(comments_data)
    
    # Upload to Firestore
    for _, row in df.iterrows():
        data = {
            'video_id': row['video_id'],
            'comment': row['comment']
        }
        db.collection("comments").add(data)

    print("Comments uploaded successfully!")

# Run the function every 5 minutes (adjust as needed)
while True:
    fetch_and_upload_comments()
    time.sleep(300)  # Wait for 5 minutes before running again
