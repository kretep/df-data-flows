from dotenv import load_dotenv
load_dotenv()

import os
from datetime import timedelta
from prefect import flow, task
from common.api_utils import fetch_text

@task(
    persist_result=True,
    cache_key_fn=lambda _1, _2: "sunspot_number",
    cache_expiration=timedelta(3 * 60 * 60),
    retries=3,
    retry_delay_seconds=5,
)
def get_sunspot_number() -> int:
    url = os.getenv("DF_SUNSPOT_NUMBER_URL")
    print(f"Fetching sunspot number from: {url}")
    data = fetch_text(url)
    sunspot_number = int(data.split('\n')[-2].split(',')[4].strip())
    return sunspot_number

@flow(name="Sunspot Number Fetcher")
def main():
    sunspot_number = get_sunspot_number()
    print(f"Current Sunspot Number: {sunspot_number}")

if __name__ == "__main__":
    main()
