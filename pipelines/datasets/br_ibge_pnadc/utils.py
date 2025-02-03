# -*- coding: utf-8 -*-
from datetime import datetime


def get_extraction_year() -> int:
    current_year = datetime.now().year

    if datetime.now().month <= 4:
        current_year -= 1

    return current_year
