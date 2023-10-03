# -*- coding: utf-8 -*-
# import datetime
# import os

# import basedosdados as bd
# import pandas as pd

# from pipelines.datasets.br_cgu_servidores_executivo_federal.constants import constants
# from pipelines.datasets.br_cgu_servidores_executivo_federal.utils import (
#     build_urls,
#     download_zip_files_for_sheet,
#     get_csv_file_by_table_name_and_date,
#     get_source,
# )
# from pipelines.utils.apply_architecture_to_dataframe.utils import (
#     read_architecture_table,
#     rename_columns,
# )
# from pipelines.utils.utils import to_partitions


# def download_files(date_start: datetime.date, date_end: datetime.date):
#     date_range: list[datetime.date] = pd.date_range(
#         date_start, date_end, freq="MS"
#     ).to_list()

#     dates_before_2020 = [date for date in date_range if date.year < 2020]
#     dates_pos_2019 = [date for date in date_range if date.year > 2019]

#     urls_for_sheets_before_2020 = {
#         sheet: build_urls(sheet, dates_before_2020)
#         for sheet in ["Militares", "Servidores_BACEN", "Servidores_SIAPE"]
#     }

#     urls_for_sheets_after_2019 = {
#         sheet: build_urls(sheet, dates_pos_2019) for sheet in constants.SHEETS.value[0]
#     }

#     for key in urls_for_sheets_after_2019.keys():
#         if key in urls_for_sheets_before_2020:
#             urls_for_sheets_after_2019[key].extend(urls_for_sheets_before_2020[key])

#     valid_sheets = {
#         sheet: payload
#         for (sheet, payload) in urls_for_sheets_after_2019.items()
#         if len(payload) > 0
#     }

#     print(f"{valid_sheets=}")

#     if not os.path.exists(constants.INPUT.value):
#         os.mkdir(constants.INPUT.value)

#     for sheet_name in valid_sheets:
#         download_zip_files_for_sheet(sheet_name, valid_sheets[sheet_name])

#     return valid_sheets


# def clean(table_name, year, month):
#     sources = constants.TABLES.value[table_name]

#     date = datetime.date(year=year, month=month, day=1)

#     csv_file = get_csv_file_by_table_name_and_date(table_name, date)

#     url_architecture = constants.ARCH.value[table_name]

#     df_architecture = read_architecture_table(url_architecture)

#     def clean_by_source(source: str):
#         path = f"{constants.INPUT.value}/{source}/{year}-{month}/{csv_file}"

#         if not os.path.exists(path):
#             return pd.DataFrame()

#         df = pd.read_csv(
#             path,
#             sep=";",
#             encoding="latin-1",
#         )

#         if "origem" in df_architecture["name"].to_list():
#             df["origem"] = get_source(table_name, source)

#         return df

#     dfs = [clean_by_source(source) for source in sources]

#     df = pd.concat(dfs).rename(columns=lambda col: col.replace("\x96 ", ""))

#     df = rename_columns(df, df_architecture)

#     df["ano"] = date.year
#     df["mes"] = date.month

#     output_dir = f"{constants.OUTPUT.value}/{table_name}"

#     if not os.path.exists(output_dir):
#         os.makedirs(output_dir, exist_ok=True)

#     # path_to_parquet = f"{output_dir}/data.csv"
#     # df.to_csv(path_to_parquet, index=False)

#     to_partitions(df, partition_columns=["ano", "mes"], savepath=output_dir)

#     path = f"{output_dir}/ano={year}/mes={month}/data.csv"

#     if os.path.exists(path):
#         return path
#     else:
#         return None


# download_files(
#     date_start=datetime.date(2013, 1, 1), date_end=datetime.date(2013, 12, 1)
# )


# def upload(table_id: str, year: int):
#     st = bd.Storage(dataset_id="br_cgu_servidores_executivo_federal", table_id=table_id)

#     for month in range(1, 12 + 1):
#         path = clean(table_id, year, month)
#         if path is None:
#             pass
#         else:
#             st.upload(
#                 path=path,
#                 mode="staging",
#                 partitions=dict(ano=str(year), mes=str(month)),
#                 if_exists="replace",
#             )


# for table in [
#     "servidores_cadastro",
#     "remuneracao",
#     "observacoes",
#     "afastamentos",
#     "aposentados_cadastro",
#     "pensionistas_cadastro",
#     "reserva_reforma_militares_cadastro",
# ]:
#     upload(table, 2023)


# tb = bd.Table("br_cgu_servidores_executivo_federal", "servidores_cadastro")

# for i in range(1, 12 + 1):
#     clean("servidores_cadastro", 2013, i)

# # path = clean("afastamentos", 2017, 1)

# tb.create(
#     path="/home/pedro/Downloads/cgu/output/servidores_cadastro/",
#     if_table_exists="replace",
#     if_storage_data_exists="replace",
# )
