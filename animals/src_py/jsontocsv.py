import os
import json
import pandas as pd
from datetime import datetime

# Get the absolute path of the script's directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Get the category directory (one level up from the script)
CATEGORY_DIR = os.path.dirname(SCRIPT_DIR)

def convert_json_to_csv():
    # Define input and output paths
    json_file = os.path.join(CATEGORY_DIR, 'data_json', 'videos_2024-10-15.json')
    output_csv = os.path.join(CATEGORY_DIR, 'data_csv', 'videos_detail.csv')
    
    # Read JSON file
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # List to store all video data
    all_videos = []
    
    # Process each channel's videos
    for channel_id, videos in data.items():
        for video in videos:
            try:
                video_data = {
                    'video_id': video.get('id', ''),
                    'title': video.get('snippet', {}).get('title', ''),
                    'description': video.get('snippet', {}).get('description', ''),
                    'title_description': f"{video.get('snippet', {}).get('title', '')} {video.get('snippet', {}).get('description', '')}",
                    'label1': '',  # You can add logic to derive labels if needed
                    'label2': '',  # You can add logic to derive labels if needed
                    'upload_date': video.get('snippet', {}).get('publishedAt', ''),
                    'channel_id': channel_id,
                    'view_count': video.get('statistics', {}).get('viewCount', 0),
                    'like_count': video.get('statistics', {}).get('likeCount', 0),
                    'comment_count': video.get('statistics', {}).get('commentCount', 0),
                    'duration': video.get('contentDetails', {}).get('duration', ''),
                    'privacy_status': video.get('status', {}).get('privacyStatus', ''),
                    'topic_categories': ','.join(video.get('topicDetails', {}).get('topicCategories', []))
                }
                all_videos.append(video_data)
            except Exception as e:
                print(f"Error processing video: {e}")
                continue
    
    # Convert to DataFrame
    df = pd.DataFrame(all_videos)
    
    # Clean upload_date format if needed
    df['upload_date'] = pd.to_datetime(df['upload_date']).dt.strftime('%Y-%m-%d')
    
    # Save to CSV
    df.to_csv(output_csv, index=False)
    print(f"CSV file saved to: {output_csv}")
    print(f"Total videos processed: {len(df)}")

if __name__ == "__main__":
    convert_json_to_csv()