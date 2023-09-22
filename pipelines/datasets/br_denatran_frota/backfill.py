# -*- coding: utf-8 -*-
from pipelines.datasets.br_denatran_frota.handlers import crawl, get_desired_file
from pipelines.datasets.br_denatran_frota.constants import constants

months = range(1, 13)
year = 2003
for month in months:
    print(month)
    crawl(month=month, year=year, temp_dir="DENATRAN_FILES")
    get_desired_file(year=year, download_directory=constants.DOWNLOAD_PATH.value)
