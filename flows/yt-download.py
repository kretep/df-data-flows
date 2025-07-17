# %%
from dotenv import load_dotenv
load_dotenv()
from prefect import flow, task
import requests
import time
import re
import json
import os

# %%
# Generic (non-API) helper functions

def get_valid_filename(name: str) -> str:
    name = name.strip()
    return re.sub(r'(?u)[\\/:*?\"<>|]', '-', name)
# print(get_valid_filename("ab.c-es(e)gs/eg   e\eg"))

def is_already_processed(videoId) -> bool:
    with open("downloaded.txt", "r") as downloaded_files_list:
        downloaded_vids = downloaded_files_list.readlines()
    for line in downloaded_vids:
        if videoId in line:
            return True
    return False

@task
def download_file(file_url, file_name) -> None:
    print("Downloading to", file_name)
    response = requests.get(file_url)
    with open(file_name, 'wb') as target_file:
        target_file.write(response.content)

@task
def log_download(videoId, file_title) -> None:
    with open("downloaded.txt", "a") as downloaded_files_list:
        downloaded_files_list.write(f'{videoId} {file_title}\n')


# %%
@task
def api_get_playlist_videos(playlist_id, api_key):
    url = "https://youtube-data8.p.rapidapi.com/playlist/videos/"
    querystring = {"id":playlist_id, "hl":"en", "gl":"US"}
    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "youtube-data8.p.rapidapi.com"
    }
    response = requests.request("GET", url, headers=headers, params=querystring)
    print(f"API response status code: {response.status_code}")
    return response.json()


# %%
@task
def api_video_to_mp3(video_id: str, api_key: str) -> dict:
    url = "https://youtube-mp36.p.rapidapi.com/dl"
    querystring = {"id": video_id}
    headers = {
      "X-RapidAPI-Key": api_key,
      "X-RapidAPI-Host": "youtube-mp36.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers, params=querystring)
    return response.json()


# %%
@flow
def download_yt_video(video, api_key) -> None:
    videoId = video["videoId"]
    video_url = f'https://www.youtube.com/watch?v={videoId}'
    file_title = f'{video["author"]["title"]} - {video["title"]}'
    print('Downloading', file_title, video_url)

    file_title = get_valid_filename(file_title)
    print(file_title)

    # Start conversion
    data = api_video_to_mp3(videoId, api_key)
    print(json.dumps(data))

    # Wait until video has been converted on server
    while not 'link' in data or data['link'] == "":
        time.sleep(15)
        data = api_video_to_mp3(videoId, api_key)
        print(json.dumps(data))

    download_url = data['link']
    print("Download available at:", download_url)
    download_file(download_url, f'{file_title}.mp3')
    log_download(videoId, file_title)


# %%
@flow
def download_videos_from_playlist():
    YT_PLAYLIST_ID = os.getenv("YT_PLAYLIST_ID")
    YT_PLAYLIST_API_KEY = os.getenv("YT_PLAYLIST_API_KEY")
    YT_MP3_API_KEY = os.getenv("YT_MP3_API_KEY")

    video_list = api_get_playlist_videos(playlist_id=YT_PLAYLIST_ID, api_key=YT_PLAYLIST_API_KEY)["contents"]
    print("Found", len(video_list), "videos in playlist")
    for item in video_list:
        video = item["video"]
        if not is_already_processed(video["videoId"]):
            download_yt_video(video, YT_MP3_API_KEY)
    print("Finished processing")

# %%
if __name__ == "__main__":
    download_videos_from_playlist()
