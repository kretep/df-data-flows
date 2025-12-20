
from dotenv import load_dotenv
load_dotenv()

import locale
from prefect import flow, task
from datetime import datetime
from PIL import Image

from nightscout import get_nightscout_data
from weerlive import fetch_current_weerlive_data
from common.ephem_utils import get_ephem_data
from birthday_notify import get_todays_birthdays
from sunspot_number import get_sunspot_number
from sunspot_image import get_sunspot_image
from kpindex_notify import get_kp_index
from buienradar import fetch_and_process_buienradar_data
from knmi_data import fetch_knmi_warnings

from eidash.draw.hkdraw import HKDraw
from eidash.esp32_client import send_image


@task
def fetch_data() -> dict:
    dataSources = {
        "nightscout": get_nightscout_data(),
        "weather": fetch_current_weerlive_data(),
        "knmi_warnings": fetch_knmi_warnings(),
        "ephem": get_ephem_data(),
        "birthdays": get_todays_birthdays(),
        "sunspot_image": get_sunspot_image(),
        "sunspot_number": get_sunspot_number(),
        "kp_index": get_kp_index(),
        "buienradar_text": fetch_and_process_buienradar_data(),
    }
    return dataSources

@task
def draw_data(data: dict) -> Image.Image:
    hkdraw = HKDraw(width=800, height=480, font_dir='src/flows/eidash/fonts')
    if data is None:
        hkdraw.clear_image()
    else:
        hkdraw.draw_data(data)
    return hkdraw.context.image

@task
def send_image_task(image: Image.Image):
    send_image(image)

def is_standby_time() -> bool:
    """Determine if the current time is within the standby period.
    
    Returns:
        True if within standby hours, False otherwise.
    """
    now = datetime.now()
    return now.hour < 7

@flow(name="E-Ink Dashboard workflow")
def eidash_workflow():
    #locale.setlocale(locale.LC_TIME, "nl_NL.utf8")
    is_standby = is_standby_time()

    # Fetch data
    data = None if is_standby else fetch_data()

    # Draw image
    image = draw_data(data)

    # Write image to local file for debugging
    image_path = "eidash_output.png"
    image.save(image_path)

    # Send image to ESP32 device
    send_image_task(image)


if __name__ == "__main__":
    eidash_workflow()

