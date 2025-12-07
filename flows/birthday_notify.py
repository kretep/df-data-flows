from dotenv import load_dotenv
load_dotenv()

from prefect import flow, task
import requests
import os
from datetime import date
from common.ntfy_utils import send_notification


@task
def get_birthdays(month: int, day: int) -> dict:
    postgrest_url = os.getenv("POSTGREST_URL")
    url = f"{postgrest_url}/birthdays?month=eq.{month}&day=eq.{day}"
    print(f"Fetching birthdays from: {url}")
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Failed to fetch birthdays: {response.status_code} - {response.text}")
        raise Exception("Failed to fetch birthdays")

    return response.json()


@task
def get_todays_birthdays() -> dict:
    today = date.today()
    return get_birthdays(today.month, today.day)


@task
def send_birthday_notification(name, year):
    message = f"🎉 It's {name}'s birthday today!"
    if year:
        age = date.today().year - year
        message += f" ({age})"

    send_notification("birthdays", "Birthday Reminder", message, priority=3)


@flow
def birthday_notify():
    birthdays = get_todays_birthdays()
    if not birthdays:
        print("No birthdays today.")
        return

    for person in birthdays:
        send_birthday_notification(person["name"], person.get("year"))

if __name__ == "__main__":
    birthday_notify()
