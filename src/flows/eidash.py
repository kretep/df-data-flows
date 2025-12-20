
import functools
import logging
import traceback
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

    # Prepare to collect all data fetching calls to execute (later, with proper error handling)
    data_calls = []
    def add_call(key, func, *args, **kwargs):
        data_calls.append({
            "key": key, "name": func.__name__, "call": functools.partial(func, *args, **kwargs)
        })

    # Add the calls
    add_call("nightscout", get_nightscout_data)
    add_call("weather", fetch_current_weerlive_data)
    add_call("knmi_warnings", fetch_knmi_warnings)
    add_call("ephem", get_ephem_data)
    add_call("birthdays", get_todays_birthdays)
    add_call("sunspot_image", get_sunspot_image)
    add_call("sunspot_number", get_sunspot_number)
    add_call("kp_data", get_kp_index)
    add_call("buienradar_text", fetch_and_process_buienradar_data)
    
    # Execute all data fetching calls with error handling
    collected_data = {}
    for call in data_calls:
        logging.info("CALLING " +call["name"])
        try:
            collected_data[call["key"]] = call["call"]()   # Execute the functools.partial
        except Exception as err:
            collected_data[call["key"]] = {
                "error": str(err)
            }
            print(traceback.format_exc())

    return collected_data

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
    locale.setlocale(locale.LC_TIME, "nl_NL.utf8")
    is_standby = is_standby_time()

    # Fetch data
    data = None if is_standby else fetch_data()

    # Draw image
    image = draw_data(data)

    # Write image to local file for debugging
    # image_path = "eidash_output.png"
    # image.save(image_path)

    # Send image to ESP32 device
    send_image_task(image)


if __name__ == "__main__":
    eidash_workflow()

