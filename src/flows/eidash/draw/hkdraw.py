import os
import logging
import functools
import traceback
from PIL import Image, ImageDraw, ImageFont

from .drawcontext import DrawContext
from .date_time import *
from .moonphase import *
from .nightscout import *
from .weather import *
from .birthdays import *
from .sunspots import draw_sunspot_number, draw_sunspot_image, draw_kp_index
from .buienradar import draw_buienradar_chart
from .planets import draw_planets


class HKDraw:

    def __init__(self, width, height, font_dir='fonts'):
        self.context = DrawContext(width, height)
        self.context.font_normal = ImageFont.truetype(os.environ['EI_FONT'], size=30)
        self.context.font_small = ImageFont.truetype(os.environ['EI_FONT'], size=18)
        self.context.font_time = self.context.font_normal
        self.context.font_weather_icons = ImageFont.truetype(os.path.join(font_dir, 'weather-iconic.ttf'), size=80)
        self.context.font_icons_small = ImageFont.truetype(os.path.join(font_dir, 'weather-iconic.ttf'), size=40)

    def clear_image(self):
        self.context.draw.rectangle((0, 0, self.context.width, 
            self.context.height), fill=self.context.white)

    def draw_error(self, message, x, y):
        self.context.draw.text((x, y), f"Error: {message}", font=self.context.font_small, fill=self.context.black)

    def draw_data(self, data):
        context = self.context
        logging.info("Drawing data")

        # Clear
        self.clear_image()

        # Collect all draw calls to execute later
        draw_calls = []
        def add_call(func, context, data, x, y, *args, **kwargs):
            draw_calls.append({
                "name": func.__name__, "context": context, "data": data, "x": x, "y": y,
                "call": functools.partial(func, context, data, x, y, *args, **kwargs)
            })

        # Date & time
        add_call(draw_date, context, {}, 10, 4)
        add_call(draw_time, context, {}, self.context.width - 4, 4)
        
        # Weather
        y1 = 60
        w1 = 140
        x1 = (self.context.width - 3 * w1) / 2
        weatherData = data.get("weather", None)
        warningData = data["knmi_warnings"]
        add_call(draw_current, context, weatherData, x1, y1, w1, 80)
        add_call(draw_temp, context, weatherData, x1+w1, y1, w1, 64)
        add_call(draw_wind, context, weatherData, x1+2*w1, y1, w1, 80, 28)
        add_call(draw_atmos, context, weatherData, 10, 250, w1, 64)
        isWarningActive = weatherData["alarm"] == "1"
        forecast_x = 90
        forecast_text_y = 150 if isWarningActive else y1 + 110
        add_call(draw_forecast_table, context, weatherData, forecast_x, y1 + 110, 100, 30)
        add_call(draw_forecast, context, {"weather": weatherData, "warning": warningData}, forecast_x + 400, forecast_text_y, 300, 0)
        if isWarningActive:
            add_call(draw_warning_symbol, context, {}, x1+3*w1, y1, 28, 6)

        # Buienradar
        add_call(draw_buienradar_chart, context, data["buienradar_text"], 10, 170, 74, 74)

        # Moon phase and planets
        ephemData = data["ephem"]
        add_call(draw_moon_phase, context, ephemData, 748, 80, 32)
        add_call(draw_planets, context, ephemData, 10, 330, 600, 130)

        # Sunspots
        add_call(draw_sunspot_image, context, data["sunspot_image"], 10, 80-36, 72, 72)
        add_call(draw_sunspot_number, context, data["sunspot_number"], 10, 120, 72, 20)
        add_call(draw_kp_index, context, data["kp_data"], 10, 140, 140, 20)

        # Nightscout
        nightScoutData = data["nightscout"]
        add_call(draw_nightscout, context, nightScoutData, 650, self.context.height-36-10, 150, 36+10)

        # Birthdays
        birthdayData = data["birthdays"]
        add_call(draw_birthdays, context, birthdayData, 100, y1-20)

        # Actually execute each draw call with proper error handling
        for call in draw_calls:
            logging.info("DRAWING " + call["name"])
            try:
                print(call["name"], type(call["data"]))
                if "error" in call["data"]:
                    # Error in data fetching
                    self.draw_error(call["data"]["error"], call["x"], call["y"])
                elif call["data"] is None:
                    # Data was not fetched to begin with
                    self.draw_error("No data", call["x"], call["y"])
                else:
                    call["call"]()   # Execute the functools.partial
            except Exception as err:
                # Error during drawing
                self.draw_error(str(err), call["x"], call["y"])
                print(traceback.format_exc())
