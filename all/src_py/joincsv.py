import pandas as pd
import os
from pathlib import Path

def combine_video_details():
    # Define categories to process
    categories = ['animals', 'blogs', 'comedy', 'entertainment', 'gaming']
    
    # Initialize an empty list to store all dataframes
    all_dfs = []
    
    # Get the base directory (YouTube folder)
    base_dir = Path(__file__).parent.parent.parent
    
    for category in categories:
        # Construct path to videos_detail.csv for each category
        csv_path = base_dir / category / 'data_csv' / 'videos_detail.csv'
        
        try:
            # Read the CSV file
            df = pd.read_csv(csv_path)
            
            # Add a category column to identify the source
            df['category'] = category
            
            # Append to our list of dataframes
            all_dfs.append(df)
            print(f"Successfully processed {category} data with {len(df)} rows")
            
        except FileNotFoundError:
            print(f"Warning: Could not find videos_detail.csv for {category}")
        except Exception as e:
            print(f"Error processing {category}: {str(e)}")
    
    if not all_dfs:
        raise Exception("No data was found to combine")
    
    # Combine all dataframes
    combined_df = pd.concat(all_dfs, ignore_index=True)
    
    # Create output directory if it doesn't exist
    output_dir = base_dir / 'all' / 'data' / 'data_csv'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save combined dataset
    output_path = output_dir / 'combined_videos_detail.csv'
    combined_df.to_csv(output_path, index=False)
    
    print(f"\nCombined data summary:")
    print(f"Total number of videos: {len(combined_df)}")
    print(f"Videos per category:")
    print(combined_df['category'].value_counts())
    print(f"\nSaved combined data to: {output_path}")
    
    return combined_df

if __name__ == "__main__":
    try:
        combine_video_details()
    except Exception as e:
        print(f"Error: {str(e)}")