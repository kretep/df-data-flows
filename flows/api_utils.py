from prefect import flow, task
import requests

@task
def fetch_text(endpoint: str) -> str:
    response = requests.get(endpoint)
    if response.status_code != 200:
        print(f"Failed to fetch data: {response.status_code} - {response.text}")
        raise Exception("Failed to fetch data")
    return response.text.strip()

@task
def fetch_json(endpoint: str) -> dict:
    response = requests.get(endpoint)
    if response.status_code != 200:
        print(f"Failed to fetch data: {response.status_code} - {response.text}")
        raise Exception("Failed to fetch data")
    return response.json()
