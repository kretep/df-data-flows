from dotenv import load_dotenv
load_dotenv()

from datetime import timedelta
import os
import requests
import numpy as np
import cv2
from prefect import task
from prefect.cache_policies import INPUTS, TASK_SOURCE


@task(
    cache_policy=INPUTS + TASK_SOURCE,
    cache_expiration=timedelta(hours=6),
    retries=2,
    retry_delay_seconds=30
)
def fetch_sunspot_image() -> bytes:
    """Fetch the latest sunspot image from the configured URL.
    
    Returns:
        Raw image data as bytes
    """
    url = os.environ["EIDASH_SUNSPOTS_URL"]
    response = requests.get(url)
    response.raise_for_status()
    return response.content


@task(cache_policy=INPUTS + TASK_SOURCE)
def process_sunspot_image(raw_data: bytes) -> np.ndarray:
    """Process raw sunspot image data to enhance sunspots and resize.
    
    Args:
        raw_data: Raw image data as bytes
        
    Returns:
        Processed image as numpy array (72x72)
    """
    # Decode raw data
    data = np.asarray(bytearray(raw_data), dtype="uint8")
    img_original = cv2.imdecode(data, cv2.IMREAD_COLOR)

    # Grayscale & threshold
    img = cv2.cvtColor(img_original, cv2.COLOR_BGR2GRAY)
    _, img_thresh = cv2.threshold(img, 140, 255, cv2.THRESH_BINARY)

    # Process background
    bg_kernel1 = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (13, 13))
    bg_erode = cv2.erode(img_thresh, bg_kernel1) # erode captions
    bg_kernel2 = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (25, 25))
    bg_dilate = cv2.dilate(bg_erode, bg_kernel2) # thicken circle
    bg_invert = ~bg_dilate

    # Embiggen sunspots
    # Bigger kernel for erosion than dilation, resulting in net growth
    # proportional to the original size.
    dilate_kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (3, 3))
    erode_kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (15, 15))
    img_combine = img_thresh.copy()        # for the result
    img_dilate_cache = img_thresh.copy()   # keep cache for accumulative dilation
    # Erode and dilate alternatingly. As the outer loop progresses, only the bigger
    # spots are not dilated away and get embiggened proportionally more (inner loop)
    for i in range(1, 4):
        # Erode (embiggen) first, to make sure we don't lose the smallest spots.
        img_erode = cv2.erode(img_dilate_cache, erode_kernel)
        for j in range(i-1):
            img_erode = cv2.erode(img_erode, erode_kernel)

        # Combine the result with what we have
        img_combine = cv2.min(img_combine, img_erode)

        # Dilate (ensmallen) to remove the smallest spots for the next round
        img_dilate_cache = cv2.dilate(img_dilate_cache, dilate_kernel)

    # Combining with background results in ring outline, while keeping sunspots
    img_combine = cv2.max(img_combine, bg_invert)

    # Resize
    img_resize = cv2.resize(img_combine, (72, 72), interpolation=cv2.INTER_NEAREST)
    
    return img_resize

def get_sunspot_image() -> np.ndarray:
    """Fetch and process the latest sunspot image.
    
    Returns:
        Processed sunspot image as numpy array (72x72)
    """
    raw_data = fetch_sunspot_image()
    processed_image = process_sunspot_image(raw_data)
    return processed_image

def main():
    # Test run, write processed image to disk
    raw_data = fetch_sunspot_image()
    processed_image = process_sunspot_image(raw_data)
    print("Processed sunspot image shape:", processed_image.shape)
    cv2.imwrite("processed_sunspot_image.png", processed_image)

if __name__ == "__main__":
    main()