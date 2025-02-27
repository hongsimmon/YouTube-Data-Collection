#!/usr/bin/python

import argparse
import json
import numpy as np
import pprint
import csv
import os
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
from datetime import datetime

# Get the absolute path of the script's directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Get the gaming directory (one level up from the script)
CATEGORY_DIR = os.path.dirname(SCRIPT_DIR)

load_dotenv() 

DEVELOPER_KEY = os.getenv('API_KEY')
YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'
now = datetime.now()
timestamp = now.strftime("%Y-%m-%d") 


def youtube_search(options):
    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
    developerKey=DEVELOPER_KEY)

    channels = {}
    
    i=0
    
    # Use the gaming directory path for CSV
    csv_path = os.path.join(CATEGORY_DIR, 'data_csv', 'channel_id.csv')
    with open(csv_path, 'r') as csvfile:
        datareader = csv.reader(csvfile)
        for row in datareader:
            search_response = youtube.channels().list(
                id=row,
                part='brandingSettings,contentDetails,contentOwnerDetails,id,localizations,snippet,statistics,status,topicDetails'
            ).execute()
            channels[i] = search_response.get('items',[i+1])[0]
            i = i+1    
    
    # Create the data_json directory if it doesn't exist
    json_dir = os.path.join(CATEGORY_DIR, 'data_json')
    os.makedirs(json_dir, exist_ok=True)
    
    file_name = f'channels_{timestamp}.json'
    file_path = os.path.join(json_dir, file_name)

    with open(file_path, 'w') as f:
       json_object = json.dumps(channels, indent = 4)
       z = json.loads(json_object)
       json.dump(z, f, indent = 4)
    print(f"Data saved to {file_path}")
    print(type(channels[1]))
    print('file dumped')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--q', help='Search term', default='ft.')
    parser.add_argument('--max-results', help='Max results', default=25)
    args = parser.parse_args()

try:
    youtube_search(args)
except HttpError as e:
    print('An HTTP error %d occurred:\n%s' % (e.resp.status, e.content))
