from PIL import Image
import numpy as np

def draw_sunspot_image(context, data, x, y, w, h):
    if isinstance(data, np.ndarray):
        img = Image.fromarray(data)
        region = (x, y, x + w, y + h)
        context.image.paste(img, region)

def draw_sunspot_number(context, data, x, y, w, h):
    text = str(data["sunspot_number"])
    context.image_text.write_text_box(x, y, text, w,
        font=context.font_small, color=context.black, align='center')

def draw_kp_index(context, kp_data, x, y, w, h):
    kp_index = kp_data["kp_index"]
    text = f'Kp {kp_index}'
    context.image_text.write_text_box(x, y, text, w,
        font=context.font_small, color=context.black)
