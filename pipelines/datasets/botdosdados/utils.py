# -*- coding: utf-8 -*-
"""
utils for botdosdados
"""

# pylint: disable=C0103

import basedosdados as bd
from PIL import Image, ImageDraw, ImageFont
import pandas as pd


def get_storage_blobs(dataset_id: str, table_id: str) -> list:
    """
    Get all blobs from a table in a dataset.
    """

    storage = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    return list(
        storage.client["storage_staging"]
        .bucket(storage.bucket_name)
        .list_blobs(prefix=f"staging/{storage.dataset_id}/{storage.table_id}/")
    )


def get_concat_v(im1, im2):
    """
    Append two images vertically
    """
    dst = Image.new("RGB", (im1.width, im1.height + im2.height))
    dst.paste(im1, (0, 0))
    dst.paste(im2, (0, im1.height))
    return dst


def get_concat_h(im1, im2):
    """
    Append two images horizontally
    """
    dst = Image.new("RGB", (im1.width + im2.width, im1.height))
    dst.paste(im1, (0, 0))
    dst.paste(im2, (im1.width, 0))
    return dst


# pylint: disable=R0913
def create_image(text, size, font_size, font_path, image_path, center_text):
    """
    Create an image with a text.
    """
    img = Image.new("RGB", size, color=(37, 42, 50))
    fnt = ImageFont.truetype(font_path, font_size)
    if center_text:
        W, H = size
        draw = ImageDraw.Draw(img)
        w, h = draw.textsize(text, font=fnt)
        draw.text(
            ((W - w) / 2, (H - h) / 2), text, spacing=40, font=fnt, fill=(255, 255, 255)
        )
    else:
        d = ImageDraw.Draw(img)
        d.text((0, 0), text, spacing=40, font=fnt, fill=(255, 255, 255))

    img.save(image_path)


def format_float_as_percentage(f):
    """
    Format float as percentage
    """
    return f"{f:.2f}%".replace(".", ",")


def add_margin(pil_img, top, right, bottom, left, color):
    """
    Add margin to an image
    """
    width, height = pil_img.size
    new_width = width + right + left
    new_height = height + top + bottom
    result = Image.new(pil_img.mode, (new_width, new_height), color)
    result.paste(pil_img, (left, top))
    return result
