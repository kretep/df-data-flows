import requests
import base64
import dotenv
import os
from datetime import date


def send_birthday_notification(name, year):
    print(f"Sending notification for {name}...")

    username = os.getenv("NTFY_USERNAME")
    password = os.getenv("NTFY_PASSWORD")
    credentials = f"{username}:{password}"
    encoded_credentials = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

    headers = {
        "Title": "Birthday reminder",
        "Priority": "3",
        "Authorization": f"Basic {encoded_credentials}"
    }

    message = f"🎉 It's {name}'s birthday today!"
    if year:
        age = date.today().year - year
        message += f" ({age})"

    host = os.getenv("NTFY_HOST")
    topic = os.getenv("NTFY_TOPIC")
    ntfy_url = f"{host}/{topic}"

    requests.post(ntfy_url, data=message.encode("utf-8"), headers=headers)


def main():
    dotenv.load_dotenv()

    today = date.today()
    month = today.month
    day = today.day

    postgrest_url = os.getenv("POSTGREST_URL")
    url = f"{postgrest_url}/birthdays?month=eq.{month}&day=eq.{day}"
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Failed to fetch birthdays: {response.status_code} - {response.text}")
        return

    birthdays = response.json()
    if not birthdays:
        print("No birthdays today.")
        return

    for person in birthdays:
        send_birthday_notification(person["name"], person.get("year"))

if __name__ == "__main__":
    main()
