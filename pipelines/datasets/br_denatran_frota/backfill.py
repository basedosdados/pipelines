# -*- coding: utf-8 -*-
from pipelines.datasets.br_denatran_frota.handlers import (
    crawl,
    treat_uf_tipo,
    get_desired_file,
    output_file_to_csv,
)
from pipelines.datasets.br_denatran_frota.constants import constants

# Fill for UF TIPO
months = range(1, 13)
years = range(2003, 2023)
for year in years:
    for month in months:
        print(month)
        crawl(month=month, year=year, temp_dir="DENATRAN_FILES")
        file = get_desired_file(
            year=year,
            download_directory="DENATRAN_FILES",
            filetype=f"{constants.UF_TIPO_BASIC_FILENAME.value}_{month}",
        )
        if year == 2004 and month == 3:
            breakpoint()
        df = treat_uf_tipo(file=file)
        path = output_file_to_csv(
            df=df, filename=constants.UF_TIPO_BASIC_FILENAME.value
        )
