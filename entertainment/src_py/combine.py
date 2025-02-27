import json
import glob
import os
from typing import Dict, Any, Iterator, Tuple
from datetime import datetime

# Get absolute path to the script's directory (gaming/src_py/)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Get absolute path to the gaming directory (parent of src_py)
BASE_DIR = os.path.dirname(SCRIPT_DIR)
now = datetime.now()
timestamp = now.strftime("%Y-%m-%d")

def process_batch_file(file_path: str) -> Iterator[Tuple[str, dict]]:
    """
    Process a single batch file and yield key-value pairs.
    
    Args:
        file_path: Path to the batch JSON file
        
    Yields:
        Tuple of (key, value) for each entry in the JSON file
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            print(f"Processing {os.path.basename(file_path)}")
            batch_data = json.load(f)
            for key, value in batch_data.items():
                yield key, value
    except json.JSONDecodeError as e:
        print(f"Error reading {file_path}: {str(e)}")
    except Exception as e:
        print(f"Unexpected error processing {file_path}: {str(e)}")

def combine_batch_files(
    input_dir: str = os.path.join(BASE_DIR, "data_json", f"batch_{timestamp}"),
    output_file: str = os.path.join(BASE_DIR, "data_json", f"videos_{timestamp}.json"),
    file_pattern: str = "videos_batch_*.json",
    chunk_size: int = 1000
) -> None:
    """
    Combines multiple batch JSON files into a single consolidated JSON file.
    Processes files incrementally to minimize memory usage.
    
    Args:
        input_dir: Directory containing the batch JSON files
        output_file: Path for the output consolidated JSON file
        file_pattern: Pattern to match batch JSON files
        chunk_size: Number of entries to process before writing to disk
        
    Raises:
        FileNotFoundError: If no batch files are found
    """
    # Get all batch files matching the pattern
    batch_files = sorted(
        glob.glob(os.path.join(input_dir, file_pattern)),
        key=lambda x: int(x.split('_')[-1].split('.')[0])  # Sort by batch number
    )
    
    if not batch_files:
        raise FileNotFoundError(f"No batch files found matching pattern: {file_pattern}")
    
    print(f"Found {len(batch_files)} batch files")
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Initialize the output file with an empty JSON object
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('{\n')
    
    total_entries = 0
    is_first_entry = True
    
    # Process each batch file
    for batch_file in batch_files:
        current_chunk = {}
        chunk_count = 0
        
        # Process entries from the current batch file
        for key, value in process_batch_file(batch_file):
            current_chunk[key] = value
            chunk_count += 1
            
            # Write chunk when it reaches the specified size
            if chunk_count >= chunk_size:
                write_chunk_to_file(output_file, current_chunk, is_first_entry)
                is_first_entry = False
                total_entries += chunk_count
                current_chunk = {}
                chunk_count = 0
        
        # Write any remaining entries in the current batch
        if current_chunk:
            write_chunk_to_file(output_file, current_chunk, is_first_entry)
            is_first_entry = False
            total_entries += chunk_count
    
    # Close the JSON object
    with open(output_file, 'a', encoding='utf-8') as f:
        f.write('\n}')
    
    print(f"\nSuccessfully combined {len(batch_files)} batch files into {output_file}")
    print(f"Combined data contains {total_entries} entries")

def write_chunk_to_file(output_file: str, chunk: Dict[str, Any], is_first_entry: bool) -> None:
    """
    Write a chunk of data to the output file.
    
    Args:
        output_file: Path to the output file
        chunk: Dictionary containing the data chunk
        is_first_entry: Whether this is the first entry in the file
    """
    with open(output_file, 'a', encoding='utf-8') as f:
        chunk_json = json.dumps(chunk)[1:-1]  # Remove the surrounding {} as we're writing entries
        if not is_first_entry:
            f.write(',\n')
        f.write(chunk_json)

if __name__ == "__main__":
    try:
        # For videos
        combine_batch_files(
            input_dir=os.path.join(BASE_DIR, "data_json", f"batch_{timestamp}"),
            output_file=os.path.join(BASE_DIR, "data_json", f"videos_{timestamp}.json"),
            file_pattern="videos_batch_*.json"
        )
        
        # For playlists
        # combine_batch_files(
        #     input_dir=os.path.join(BASE_DIR, "data_json", f"batch_{timestamp}"),
        #     output_file=os.path.join(BASE_DIR, "data_json", f"playlists_{timestamp}.json"),
        #     file_pattern="playlists_batch_*.json"
        # )
    except Exception as e:
        print(f"An error occurred: {str(e)}")
