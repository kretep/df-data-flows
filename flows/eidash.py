
from dotenv import load_dotenv
load_dotenv()

import os
from prefect import flow, task
from datetime import datetime, timezone
from PIL import Image

from nightscout import get_nightscout_data
from weerlive import fetch_current_weerlive_data
from common.ephem_utils import get_ephem_data
from birthday_notify import get_todays_birthdays
from sunspot_number import get_sunspot_number
#from sunspot_image import get_sunspot_image
from kpindex_notify import get_kp_index
#from buienradar import get_buienradar_text

from eidash.draw.hkdraw import HKDraw
from eidash.esp32_client import send_image


@task
def fetch_data():
    dataSources = {
        "nightscout": get_nightscout_data(),
        "weather": fetch_current_weerlive_data(),
        "ephem": get_ephem_data(),
        "birthdays": get_todays_birthdays(),
        #"sunspot_image": get_sunspot_image(),
        "sunspot_number": get_sunspot_number(),
        "kp_index": get_kp_index(),
        #"buienradar_text": get_buienradar_text(),
    }
    return dataSources

@task
def draw_data(data) -> Image.Image:
    hkdraw = HKDraw(width=800, height=480, font_dir='flows/eidash/fonts')
    hkdraw.draw_data(data)
    return hkdraw.context.image


@task
def send_image_task(image):
    send_image(image)


@flow(name="E-Ink Dashboard workflow")
def eidash_workflow():
    data = fetch_data()
    image = draw_data(data)

    # Write image to local file for debugging
    image_path = "eidash_output.png"
    image.save(image_path)

    # send_image(image)


if __name__ == "__main__":
    eidash_workflow()

