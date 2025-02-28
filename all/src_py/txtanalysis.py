import csv
import re

# Placeholder API Key
def get_video_url(video_id):
    """Constructs a YouTube video URL given the video ID."""
    return f"https://www.youtube.com/watch?v={video_id}"

# Read and parse the input file
input_file = 'gaming.txt'
output_file = 'gaming.csv'

data = []
with open(input_file, 'r', encoding='utf-8') as file:
    content = file.read()
    entries = content.split("\nSelected index")  # Split into separate video entries
    
    for entry in entries:
        match = re.search(r"video_id='(.*?)', title='(.*?)'", entry)
        if match:
            video_id = match.group(1)
            video_title = match.group(2)
            
            # Extract collaborators from both standard format and 'Too many collaborators' format
            collaborators_match = re.findall(r"Collaborator: ([^,\n]+)", entry)
            too_many_collaborators_match = re.search(r"Too many collaborators in video .*?: \[(.*?)\]", entry)
            
            if too_many_collaborators_match:
                collaborators_match.extend(too_many_collaborators_match.group(1).replace("'", "").split(", "))
            
            num_collaborators = len(collaborators_match)
            
            # Capture only the first 5 collaborators, but count all
            displayed_collaborators = collaborators_match[:5] + [''] * (5 - min(5, num_collaborators))
            
            data.append([
                video_id, video_title, num_collaborators,
                *displayed_collaborators,  # Fill collaborators
                get_video_url(video_id)  # Correct placement of link
            ])

# Write to CSV file
headers = [
    "video_id", "video_title", "num_collaborators",
    "collaborator_1", "collaborator_2", "collaborator_3",
    "collaborator_4", "collaborator_5", "link"
]

with open(output_file, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(headers)
    writer.writerows(data)

print(f"CSV file saved to {output_file}")
