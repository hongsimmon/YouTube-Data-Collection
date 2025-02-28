import pandas as pd
from datetime import datetime
import os

# Define paths
data_path = '../data/'
csv_file_path = f"{data_path}data_csv/animals_videos_detail.csv"
output_xlsx_path = f"{data_path}data_csv/samples/animals_sample.xlsx"

# Make sure the output directory exists
os.makedirs(os.path.dirname(output_xlsx_path), exist_ok=True)

# Read the CSV file
print(f"Reading CSV file from: {csv_file_path}")
videos_df = pd.read_csv(csv_file_path)

# Display basic info about the CSV
print(f"CSV loaded. Shape: {videos_df.shape}")
print(f"Columns: {videos_df.columns.tolist()}")

# Get unique channel IDs
unique_channels = videos_df['channel_id'].unique()
print(f"Found {len(unique_channels)} unique channels")

# Create a new dataframe for the Excel file
result_df = pd.DataFrame(columns=['video_id', 'channel_id', 'videoCount'])

# For each channel, find a video and count videos from January 2024
for channel_id in unique_channels:
    # Filter videos for this channel
    channel_videos = videos_df[videos_df['channel_id'] == channel_id]
    
    # Get the first video_id for this channel (or you could choose any specific criteria)
    if not channel_videos.empty:
        video_id = channel_videos.iloc[0]['video_id']
        
        # Count videos from this channel posted since January 2024
        if 'upload_date' in videos_df.columns:
            # Filter for videos in 2024
            jan_2024 = datetime.strptime('2024-01-01', '%Y-%m-%d')
            
            # Convert upload_date to datetime for comparison
            channel_videos['upload_date_dt'] = pd.to_datetime(channel_videos['upload_date'], errors='coerce')
            
            # Count videos published in 2024
            videos_2024_count = len(channel_videos[channel_videos['upload_date_dt'] >= jan_2024])
        else:
            # If upload_date column doesn't exist, use total count as fallback
            videos_2024_count = len(channel_videos)
            print(f"Warning: 'upload_date' column not found. Using total count for channel {channel_id}")
        
        # Add to results dataframe
        result_df = result_df._append({
            'video_id': video_id,
            'channel_id': channel_id,
            'videoCount': videos_2024_count
        }, ignore_index=True)

# Save to Excel
print(f"Saving Excel file to: {output_xlsx_path}")
result_df.to_excel(output_xlsx_path, index=False)
print(f"Excel file created successfully with {len(result_df)} rows")

# Preview the result
print("\nPreview of the created Excel file:")
print(result_df.head())