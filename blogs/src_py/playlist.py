#!/usr/bin/python

import argparse
import json
import csv
import os
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime, timezone
from dotenv import load_dotenv

# Get the absolute path of the script's directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Get the category directory (one level up from the script)
CATEGORY_DIR = os.path.dirname(SCRIPT_DIR)

load_dotenv()

DEVELOPER_KEY = os.getenv('API_KEY')
YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'
CUTOFF_DATE = datetime(2024, 5, 1, tzinfo=timezone.utc)
now = datetime.now()
timestamp = now.strftime("%Y-%m-%d") 

# Create timestamp directory with absolute path
timestamp_dir = os.path.join(CATEGORY_DIR, 'data_json', f'batch_{timestamp}')
os.makedirs(timestamp_dir, exist_ok=True)

# Add counters for logging
class Stats:
    def __init__(self):
        self.total_videos = 0
        self.processed_playlists = 0

stats = Stats()

def get_video_ids(youtube, playlist_id):
    video_ids = []
    next_page_token = None

    try:
        while True:
            request = youtube.playlistItems().list(
                part='contentDetails,snippet',
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()

            items = response['items']
            published_dates = [
                datetime.strptime(item['snippet']['publishedAt'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                for item in items
            ]
            
            new_ids = [
                item['contentDetails']['videoId']
                for item, pub_date in zip(items, published_dates)
                if pub_date >= CUTOFF_DATE
            ]
            
            if not new_ids:
                break
                
            video_ids.extend(new_ids)
            
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break

    except HttpError as e:
        print(f"An HTTP error occurred for playlist {playlist_id}: {e.resp.status} {e.content}")
        return None

    return video_ids

def process_video_batch(youtube, video_ids_chunk):
    """
    Process a batch of videos
    """
    try:
        request = youtube.videos().list(
            part='statistics,contentDetails,snippet,status',
            id=','.join(video_ids_chunk)
        )
        response = request.execute()
        
        videos = response.get('items', [])
        stats.total_videos += len(videos)
        
        return videos
        
    except HttpError as e:
        print(f"An HTTP error occurred while fetching video details: {e.resp.status} {e.content}")
        return []

def process_playlist_batch(youtube, batch_data, batch_number):
    """
    Process a batch of playlists
    """
    current_playlists = {}
    current_videos = {}
    
    for row in batch_data:
        playlist_id = row[0]
        print(f"\nProcessing playlist: {playlist_id}")
        
        video_ids = get_video_ids(youtube, playlist_id)
        if not video_ids:
            continue
            
        current_playlists[playlist_id] = video_ids
        
        # Process videos in chunks of 50
        all_videos = []
        total_chunks = len(video_ids) // 50 + (1 if len(video_ids) % 50 else 0)
        
        for i in range(0, len(video_ids), 50):
            chunk = video_ids[i:i+50]
            chunk_number = i // 50 + 1
            print(f"Processing chunk {chunk_number}/{total_chunks} for playlist {playlist_id}")
            videos = process_video_batch(youtube, chunk)
            all_videos.extend(videos)
            
        current_videos[playlist_id] = all_videos
        stats.processed_playlists += 1
        
        # Print progress after each playlist
        print(f"\nProgress Update:")
        print(f"Videos processed: {stats.total_videos}")
        print(f"Playlists processed: {stats.processed_playlists}")
    
    # Save results with absolute paths
    if current_playlists:
        playlists_file = os.path.join(timestamp_dir, f'playlists_batch_{batch_number}.json')
        with open(playlists_file, 'w') as f:
            json.dump(current_playlists, f, indent=4)
        print(f'Playlists batch {batch_number} dumped')

        videos_file = os.path.join(timestamp_dir, f'videos_batch_{batch_number}.json')
        with open(videos_file, 'w') as f:
            json.dump(current_videos, f, indent=4)
        print(f'Videos batch {batch_number} dumped')
        
    return len(current_playlists)

def youtube_search(options, start_batch):
    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
                   developerKey=DEVELOPER_KEY)
    
    batch_size = 50
    batch_number = start_batch
    total_processed = start_batch * batch_size
    
    # Use absolute path for CSV file
    csv_path = os.path.join(CATEGORY_DIR, 'data_csv', 'playlist_id.csv')
    with open(csv_path, 'r') as csvfile:
        datareader = csv.reader(csvfile)
        all_rows = list(datareader)[total_processed:]
    
    while all_rows:
        current_batch = all_rows[:batch_size]
        all_rows = all_rows[batch_size:]
        
        if not current_batch:
            break
            
        print(f"\nProcessing batch {batch_number}")
        processed = process_playlist_batch(youtube, current_batch, batch_number)
        
        if processed > 0:
            total_processed += processed
            print(f"\nBatch Summary:")
            print(f"Total playlists processed so far: {total_processed}")
            batch_number += 1
    
    print(f"\nFinal Summary:")
    print(f"Total playlists processed: {total_processed}")
    print(f"Total batches processed: {batch_number - start_batch}")
    print(f"Total videos processed: {stats.total_videos}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--q', help='Search term', default='ft.')
    parser.add_argument('--max-results', help='Max results', default=25)
    parser.add_argument('--start-batch', type=int, default=0, help='Batch number to start from')
    args = parser.parse_args()

    try:
        youtube_search(args, args.start_batch)
    except Exception as e:
        print(f"An error occurred: {str(e)}")
