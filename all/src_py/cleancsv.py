import pandas as pd
import os
import csv
import logging
import time
from datetime import datetime

# Start timing the process
start_time = time.time()

# Set up logging
log_filename = f"normalize_csv_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Increase CSV field size limit to handle large cells
csv.field_size_limit(500000000)

# Define file paths - these are already set for your specific file
data_path = '../data/'
input_file = f"{data_path}data_csv/videos_detail_animals_20241015.csv"
output_file = f"{data_path}data_csv/normalized_videos_detail_animals_20241015.csv"

logger.info("=" * 80)
logger.info("YOUTUBE CSV NORMALIZER")
logger.info("=" * 80)
logger.info(f"Processing file: {input_file}")

# Ensure output directory exists
os.makedirs(os.path.dirname(output_file), exist_ok=True)

try:
    # Read the CSV with forgiving settings
    # Note: Removed low_memory parameter as it's not supported with python engine
    logger.info("Reading CSV file...")
    df = pd.read_csv(
        input_file, 
        engine='python',
        on_bad_lines='skip',
        dtype={
            'video_id': 'string',
            'title': 'string',
            'description': 'string',
            'title_description': 'string',
            'label1': 'string',
            'label2': 'string',
            'channel_id': 'string',
            'privacy_status': 'string',
            'topic_categories': 'string'
        }
    )
    
    original_row_count = len(df)
    original_column_count = len(df.columns)
    logger.info(f"Successfully read file with {original_row_count} rows and {original_column_count} columns")
    
    # Display column information
    logger.info("\nOriginal columns and data types:")
    for col, dtype in df.dtypes.items():
        non_null = df[col].count()
        logger.info(f"  {col}: {dtype} - {non_null} non-null values ({non_null/len(df):.1%})")
    
    # Standardize column names
    df.columns = [col.lower().strip() for col in df.columns]
    logger.info("\nStandardized column names to lowercase")
    
    # Process string columns
    string_columns = ['video_id', 'title', 'description', 'title_description', 
                      'channel_id', 'privacy_status', 'topic_categories']
    
    for col in string_columns:
        if col in df.columns:
            logger.info(f"Processing string column: {col}")
            df[col] = df[col].fillna("").astype(str)
            if df[col].str.contains('nan').any():
                df[col] = df[col].replace('nan', '')
                logger.info(f"  Replaced 'nan' strings in {col}")
    
    # Process numeric columns
    numeric_columns = ['view_count', 'like_count', 'comment_count']
    
    for col in numeric_columns:
        if col in df.columns:
            logger.info(f"Processing numeric column: {col}")
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    
    # Process date columns
    if 'upload_date' in df.columns:
        logger.info("Processing upload_date column")
        before_valid = df['upload_date'].notna().sum()
        
        # Convert to datetime
        df['upload_date'] = pd.to_datetime(df['upload_date'], errors='coerce')
        after_valid = df['upload_date'].notna().sum()
        
        logger.info(f"  Valid dates: {before_valid} before conversion, {after_valid} after conversion")
        
        # Add month column
        df['upload_month'] = df['upload_date'].dt.month
        df['upload_year'] = df['upload_date'].dt.year
        
        month_counts = df['upload_month'].value_counts().sort_index()
        logger.info(f"  Monthly distribution: {month_counts.to_dict()}")
    else:
        logger.warning("No upload_date column found")
    
    # Process collaborator data
    logger.info("Processing collaborator data")
    if 'label2' in df.columns:
        df['label2'] = df['label2'].fillna("").astype(str)
        # Replace 'nan' text with empty string
        df['label2'] = df['label2'].replace('nan', '')
        
        # Create label2_list column
        df['label2_list'] = df['label2'].apply(lambda x: x.split(',') if x and x != 'nan' else [])
        
        # Fix empty lists where split creates ['']
        df.loc[df['label2'] == '', 'label2_list'] = df.loc[df['label2'] == '', 'label2_list'].apply(lambda x: [])
        
        # Calculate collaborator count
        df['collaborator_count'] = df['label2_list'].apply(len)
        
        # Count videos with collaborators
        collab_count = (df['collaborator_count'] > 0).sum()
        logger.info(f"  Videos with collaborators: {collab_count} ({collab_count/len(df):.1%})")
    else:
        logger.warning("No label2 column found")
        df['label2'] = ""
        df['label2_list'] = [[] for _ in range(len(df))]
        df['collaborator_count'] = 0
    
    # Add category
    df['category'] = 'animals'
    
    # Save the normalized file
    logger.info(f"Saving normalized file to: {output_file}")
    df.to_csv(output_file, index=False)
    
    # Verify the saved file
    try:
        test_df = pd.read_csv(output_file)
        logger.info(f"Verification successful - saved file has {len(test_df)} rows and {len(test_df.columns)} columns")
    except Exception as e:
        logger.error(f"Verification failed - could not read saved file: {str(e)}")
    
    # Calculate processing time
    elapsed_time = time.time() - start_time
    logger.info(f"\nProcessing completed in {elapsed_time:.2f} seconds")
    
    # Provide summary of changes
    logger.info("\nNORMALIZATION SUMMARY")
    logger.info(f"Original rows: {original_row_count}")
    logger.info(f"Final rows: {len(df)}")
    logger.info(f"Original columns: {original_column_count}")
    logger.info(f"Final columns: {len(df.columns)}")
    logger.info(f"Added columns: {', '.join(set(df.columns) - set(df.columns[:original_column_count]))}")
    
    logger.info("\nThe normalized CSV file is ready to use!")
    logger.info(f"To use the normalized file in your code, replace:")
    logger.info(f"videos = pd.read_csv(f\"{{data_path}}data_csv/videos_detail_animals_20241015.csv\", engine='python')")
    logger.info(f"with:")
    logger.info(f"videos = pd.read_csv(f\"{{data_path}}data_csv/normalized_videos_detail_animals_20241015.csv\")")

except Exception as e:
    logger.error(f"Error normalizing CSV file: {str(e)}", exc_info=True)
    print(f"Error: {str(e)}")