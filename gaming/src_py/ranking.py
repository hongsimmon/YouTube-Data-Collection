import aiohttp
import asyncio
from bs4 import BeautifulSoup
import csv
import re
from urllib.parse import urlparse, parse_qs
import os
import random
from datetime import datetime

# List of user agents to rotate
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15',
]

# Define base directory and data directory
BASE_DIR = os.path.expanduser('~/YouTube/gaming')
DATA_CSV_DIR = os.path.join(BASE_DIR, 'data_csv')

now = datetime.now()
timestamp = now.strftime("%Y-%m-%d")

async def get_channel_data(session, channel_url, retries=3):
    for attempt in range(retries):
        try:
            headers = {'User-Agent': random.choice(USER_AGENTS)}
            async with session.get(channel_url, headers=headers, timeout=30) as response:
                content = await response.text()
                soup = BeautifulSoup(content, 'html.parser')
                
                profile_image_div = soup.find('div', class_='profile-image')
                if profile_image_div:
                    link_tag = profile_image_div.find('a')
                    if link_tag and 'href' in link_tag.attrs:
                        indirect_url = f"https://us.youtubers.me{link_tag['href']}"
                        return await get_direct_youtube_url(session, indirect_url)
            return None
        except asyncio.TimeoutError:
            print(f"Timeout occurred for {channel_url}. Retrying... (Attempt {attempt + 1}/{retries})")
            await asyncio.sleep(5)  # Wait for 5 seconds before retrying
    print(f"Failed to retrieve data for {channel_url} after {retries} attempts.")
    return None

async def get_direct_youtube_url(session, indirect_url, retries=3):
    for attempt in range(retries):
        try:
            headers = {'User-Agent': random.choice(USER_AGENTS)}
            async with session.get(indirect_url, headers=headers, allow_redirects=True, timeout=30) as response:
                if 'youtube.com' in str(response.url):
                    return str(response.url)
                
                content = await response.text()
                soup = BeautifulSoup(content, 'html.parser')
                
                youtube_link = soup.find('a', href=lambda href: href and 'youtube.com' in href)
                if youtube_link:
                    return youtube_link['href']
                
                for script in soup.find_all('script'):
                    if script.string and 'youtube.com/channel/' in script.string:
                        channel_id = script.string.split('youtube.com/channel/')[-1].split('"')[0]
                        return f'https://www.youtube.com/channel/{channel_id}'
            return None
        except asyncio.TimeoutError:
            print(f"Timeout occurred for {indirect_url}. Retrying... (Attempt {attempt + 1}/{retries})")
            await asyncio.sleep(5)  # Wait for 5 seconds before retrying
    print(f"Failed to retrieve direct YouTube URL for {indirect_url} after {retries} attempts.")
    return None

def extract_channel_id(channel_link):
    if channel_link:
        match = re.search(r'/channel/([^/?&]+)', channel_link)
        if match:
            return match.group(1)
        
        parsed_url = urlparse(channel_link)
        query_params = parse_qs(parsed_url.query)
        if 'channel' in query_params:
            return query_params['channel'][0]
    return None

async def scrape_youtube_channels(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            content = await response.text()
            soup = BeautifulSoup(content, 'html.parser')
            
            table = soup.find('table', class_='top-charts')
            if not table:
                print("Table not found. The website structure might have changed.")
                return []
            
            rows = table.find_all('tr')[1:]  # Skip the header row
            tasks = []
            
            for row in rows:
                columns = row.find_all('td')
                if len(columns) < 7:
                    continue
                
                rank = columns[0].text.strip()
                youtuber = columns[1].text.strip()
                subscribers = columns[2].text.strip()
                video_views = columns[3].text.strip()
                video_count = columns[4].text.strip()
                category = columns[5].text.strip()
                started = columns[6].text.strip()
                
                channel_page_link = columns[1].find('a')['href']
                channel_page_url = f"https://us.youtubers.me{channel_page_link}"
                
                task = asyncio.create_task(get_channel_data(session, channel_page_url))
                tasks.append((rank, youtuber, subscribers, video_views, video_count, category, started, task))
            
            data = []
            for rank, youtuber, subscribers, video_views, video_count, category, started, task in tasks:
                channel_link = await task
                channel_id = extract_channel_id(channel_link)
                
                data.append([rank, youtuber, subscribers, video_views, video_count, category, started, channel_link, channel_id])
                print(f"Scraped data for {youtuber}")
                    
                await asyncio.sleep(1)  # Add a delay between requests
    
    return data

def save_to_csv(data, filename):
    filepath = os.path.join(DATA_CSV_DIR, filename)
    os.makedirs(DATA_CSV_DIR, exist_ok=True)  # Ensure directory exists
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Rank', 'Youtuber', 'Subscribers', 'Video Views', 'Video Count', 'Category', 'Started', 'Channel Link', 'Channel ID'])
        writer.writerows(data)

def save_channel_ids_to_csv(data, filename):
    filepath = os.path.join(DATA_CSV_DIR, filename)
    os.makedirs(DATA_CSV_DIR, exist_ok=True)  # Ensure directory exists
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        for row in data:
            channel_id = row[8] if len(row) > 8 else ''  # Use empty string if channel_id is missing
            writer.writerow([channel_id])

async def main():
    url = 'https://us.youtubers.me/united-states/gaming/top-1000-most-subscribed-youtube-channels-in-united-states'
    youtube_data = await scrape_youtube_channels(url)

    if youtube_data:
        full_data_filename = f'channels_{timestamp}.csv'
        save_to_csv(youtube_data, full_data_filename)
        print(f"Full data has been scraped and saved to {os.path.join(DATA_CSV_DIR, full_data_filename)}")
        print(f"Total number of entries in full data: {len(youtube_data)}")

        channel_ids_filename = 'channel_id.csv'
        save_channel_ids_to_csv(youtube_data, channel_ids_filename)
        print(f"Channel IDs have been saved to {os.path.join(DATA_CSV_DIR, channel_ids_filename)}")
        
        # Count non-empty channel IDs
        non_empty_channel_ids = sum(1 for row in youtube_data if row[8])
        print(f"Number of non-empty channel IDs: {non_empty_channel_ids}")

    else:
        print("No data was scraped. Please check the website and the script.")

if __name__ == "__main__":
    asyncio.run(main())
