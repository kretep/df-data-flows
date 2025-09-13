
from dotenv import load_dotenv
load_dotenv()
from prefect import flow, task
import requests
import time
import re
import json
import os
import sys

DOWNLOADS_LIST = "downloaded.txt"

# Generic (non-API) helper functions
def get_valid_filename(name: str) -> str:
    name = name.strip()
    return re.sub(r'(?u)[\\/:*?\"<>|]', '-', name)
# print(get_valid_filename("ab.c-es(e)gs/eg   e\eg"))

def get_path_in_output_dir(file_name: str) -> str:
    output_dir = os.getenv("ZT_OUTPUT_DIR", "output/zt-download")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    return os.path.join(output_dir, file_name)

def is_already_processed(videoId) -> bool:
    path = get_path_in_output_dir(DOWNLOADS_LIST)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Downloads list not found at {path}")
    with open(path, "r") as downloaded_files_list:
        downloaded_vids = downloaded_files_list.readlines()
    for line in downloaded_vids:
        if videoId in line:
            return True
    return False

@task
def download_file(file_url, file_name) -> None:
    path = get_path_in_output_dir(file_name)
    print("Downloading to", path)
    response = requests.get(file_url)
    with open(path, 'wb') as target_file:
        target_file.write(response.content)

@task
def log_download(videoId, file_title) -> None:
    path = get_path_in_output_dir(DOWNLOADS_LIST)
    with open(path, "a") as downloaded_files_list:
        downloaded_files_list.write(f'{videoId} {file_title}\n')


@task
def api_get_playlist_videos(playlist_id, api_key):
    api_host = os.getenv("ZT_PLAYLIST_API_HOST")
    url = f"https://{api_host}/playlist/videos/"
    querystring = {"id":playlist_id, "hl":"en", "gl":"US"}
    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": api_host
    }
    response = requests.request("GET", url, headers=headers, params=querystring)
    print(f"API response status code: {response.status_code}")
    return response.json()

@task
def api_get_video_info(video_id, api_key):
    # Only used for single videos; playlist videos are fetched in bulk
    api_host = os.getenv("ZT_PLAYLIST_API_HOST")
    url = f"https://{api_host}/video/details/"
    querystring = {"id":video_id, "hl":"en", "gl":"US"}
    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": api_host
    }
    response = requests.request("GET", url, headers=headers, params=querystring)
    print(f"API response status code: {response.status_code}")
    return response.json()

@task
def api_video_to_mp3(video_id: str, api_key: str) -> dict:
    api_host = os.getenv("ZT_MP3_API_HOST")
    url = f"https://{api_host}/dl"
    querystring = {"id": video_id}
    headers = {
      "X-RapidAPI-Key": api_key,
      "X-RapidAPI-Host": api_host
    }
    response = requests.get(url, headers=headers, params=querystring)
    return response.json()


@flow
def download_zt_video(video, api_key) -> None:
    zt_host = os.getenv("ZT_HOST")
    videoId = video["videoId"]
    video_url = f'{zt_host}/watch?v={videoId}'
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


@flow
def download_videos_from_playlist():
    ZT_PLAYLIST_ID = os.getenv("ZT_PLAYLIST_ID")
    ZT_PLAYLIST_API_KEY = os.getenv("ZT_PLAYLIST_API_KEY")
    ZT_MP3_API_KEY = os.getenv("ZT_MP3_API_KEY")

    video_list = api_get_playlist_videos(playlist_id=ZT_PLAYLIST_ID, api_key=ZT_PLAYLIST_API_KEY)["contents"]
    print("Found", len(video_list), "videos in playlist")
    for item in video_list:
        video = item["video"]
        if not is_already_processed(video["videoId"]):
            download_zt_video(video, ZT_MP3_API_KEY)
    print("Finished processing")


@flow
def download_single_video(video_id: str):
    ZT_MP3_API_KEY = os.getenv("ZT_MP3_API_KEY")
    ZT_PLAYLIST_API_KEY = os.getenv("ZT_PLAYLIST_API_KEY")

    video = api_get_video_info(video_id, ZT_PLAYLIST_API_KEY)
    if not is_already_processed(video["videoId"]):
        download_zt_video(video, ZT_MP3_API_KEY)
    print("Finished processing single video")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Video ID provided as command line argument
        video_id = sys.argv[1]
        print(f"Downloading single video: {video_id}")
        download_single_video(video_id)
    else:
        # No argument provided, download the whole playlist
        print("No video ID provided, downloading entire playlist")
        download_videos_from_playlist()
