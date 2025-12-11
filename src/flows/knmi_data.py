from dotenv import load_dotenv
load_dotenv()

from prefect import flow, task
from prefect.cache_policies import TASK_SOURCE, INPUTS
import os, requests
from datetime import timedelta
from common.api_utils import fetch_json

def cache_key_fn(task, args):
    return "knmi_data"


@task(
    persist_result=True,
    cache_key_fn=cache_key_fn,
    cache_expiration=timedelta(minutes=10),
    cache_policy=TASK_SOURCE + INPUTS,
    retries=3,
    retry_delay_seconds=5,
)
def list_knmi_files() -> dict:
    """Step 1: list files (latest first)."""
    url = os.getenv("KNMI_WARNINGS_ENDPOINT")  # e.g. https://api.dataplatform.knmi.nl/open-data/v1/datasets/waarschuwingen_nederland_48h/versions/1.0/files
    params = {"sorting": "desc", "order_by": "created", "maxKeys": 1}
    api_key = os.getenv("KNMI_OPEN_DATA_API_KEY")
    headers = {"Authorization": f"Bearer {api_key}"}
    return fetch_json(url, headers=headers, params=params)

@task(retries=3, retry_delay_seconds=5)
def get_file_download_url(file_listing: dict) -> str:
    """Step 2: request temporary download URL for that file."""
    api_key = os.getenv("KNMI_OPEN_DATA_API_KEY")
    headers = {"Authorization": f"Bearer {api_key}"}
    base_url = os.getenv("KNMI_WARNINGS_ENDPOINT")
    filename = file_listing["files"][0]["filename"]
    url = f"{base_url}/{filename}/url"
    data = fetch_json(url, headers=headers)
    return data["temporaryDownloadUrl"]

@task(retries=3, retry_delay_seconds=5)
def download_knmi_file(download_url: str) -> str:
    """Step 3: download the XML/TXT content."""
    resp = requests.get(download_url, timeout=30)
    resp.raise_for_status()
    return resp.text

@task
def parse_knmi_warnings(raw_content: str) -> str:
    """Simply return the first line of the content."""
    lines = raw_content.strip().splitlines()
    return lines[0] if lines else ""

@flow
def fetch_knmi_warnings():
    files = list_knmi_files()
    url = get_file_download_url(files)
    raw = download_knmi_file(url)
    first_line = parse_knmi_warnings(raw)
    print(first_line)
    warningData = {
        "text": first_line
    }
    return warningData

if __name__ == "__main__":
    fetch_knmi_warnings()