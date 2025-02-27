import json
import csv
from datetime import datetime
import os

# Get the absolute path of the script's directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Get the category directory (one level up from the script)
CATEGORY_DIR = os.path.dirname(SCRIPT_DIR)

now = datetime.now()
timestamp = now.strftime("%Y-%m-%d") 

def extract_playlist_ids(json_file, csv_file):
    # Read the JSON file
    with open(json_file, 'r') as f:
        data = json.load(f)
    
    # Extract playlist IDs
    playlist_ids = []
    skipped_channels = 0
    missing_keys = 0
    
    for channel_id, channel in data.items():
        if isinstance(channel, dict):
            try:
                uploads_id = channel['contentDetails']['relatedPlaylists']['uploads']
                if uploads_id:
                    playlist_ids.append(uploads_id)
                else:
                    print(f"Empty uploads ID for channel: {channel_id}")
                    skipped_channels += 1
            except KeyError as e:
                print(f"Missing key for channel {channel_id}: {str(e)}")
                missing_keys += 1
        else:
            print(f"Unexpected data type for channel {channel_id}: {type(channel)}")
            skipped_channels += 1
    
    # Create the directory if it doesn't exist
    os.makedirs(os.path.dirname(csv_file), exist_ok=True)
    
    # Write playlist IDs to CSV file
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        for playlist_id in playlist_ids:
            writer.writerow([playlist_id])
    
    print(f"Playlist IDs have been written to {csv_file}")
    print(f"Total channels processed: {len(data)}")
    print(f"Playlist IDs extracted: {len(playlist_ids)}")
    print(f"Channels skipped due to unexpected data: {skipped_channels}")
    print(f"Channels skipped due to missing keys: {missing_keys}")

# Specify the input JSON file and output CSV file with absolute paths
json_file = os.path.join(CATEGORY_DIR, 'data_json', f'channels_{timestamp}.json')
csv_file = os.path.join(CATEGORY_DIR, 'data_csv', 'playlist_id.csv')

# Run the extraction
extract_playlist_ids(json_file, csv_file)
