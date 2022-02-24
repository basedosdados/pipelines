import pandas as pd
import basedosdados as bd
import time
import concurrent.futures
from vacinacao import *

start = time.perf_counter()
# nested dictionary where the uppper level keys are the level of aggregation used in the availanle link. Links information inside the inner dictionary
partition_info = {
    "partition": {
        "url": "https://opendatasus.saude.gov.br/dataset/covid-19-vacinacao/resource/301983f2-aa50-4977-8fec-cfab0806cb0b",
        "pattern": r"Dados\sCompletos\s-\sParte\s\d+",
    },
    "AC-MT": {
        "url": "https://opendatasus.saude.gov.br/dataset/covid-19-vacinacao/resource/5093679f-12c3-4d6b-b7bd-07694de54173?inner_span=True",
        "pattern": r"Dados\s[A-Z]+\s-\sParte\s\d+",
    },
    "PA-TO": {
        "url": "https://opendatasus.saude.gov.br/dataset/covid-19-vacinacao/resource/10aed154-04c8-4cf4-b78a-8f0fa1bc5af4?inner_span=True",
        "pattern": r"Dados\s[A-Z]+\s-\sParte\s\d+",
    },
}

new_dirs = ["/tmp/data","/tmp/data/br_ms_vacinacao_covid19", "/tmp/data/br_ms_vacinacao_covid19/input", "/tmp/data/br_ms_vacinacao_covid19/output/microdados",
"/tmp/data/br_ms_vacinacao_covid19/output/microdados_vacinacao", "/tmp/data/br_ms_vacinacao_covid19/output/microdados_paciente",
"/tmp/data/br_ms_vacinacao_covid19/output/microdados_estabelecimento", "/tmp/data/br_ms_vacinacao_covid19/aux"]

for new_dir in new_dirs:
  os.system(f"mkdir -p {new_dir}")


municipio = bd.read_sql("""
                          SELECT *
                          FROM basedosdados.br_bd_diretorios_brasil.municipio
                          """,
                          billing_project_id='basedosdados-dev')

municipio.to_csv("aux/municipio.csv",
                   index=False)

partitions = ["Dados PA - Parte 1", "Dados PA - Parte 2", "Dados PA - Parte 3"]

# with concurrent.futures.ProcessPoolExecutor() as executor:
#     executor.map(download_raw, partitions)

for partition in partitions[:1]:
    download_raw("PA-TO",partition, partition_info)


# os.system('''

# '''
# )


# filename = "input/" + uf + ".csv"

# print("Using raw files of {}.".format(uf))
# chunksize = 10 ** 6
# n_chunk = 1

# for df in pd.read_csv(filename,
#                         sep=";",
#                         dtype=object,
#                         chunksize=chunksize):

#     print("Cleaning state {}_{}.".format(uf, n_chunk))
#     df.fillna('')
#     build_microdados(uf, df, municipio, n_chunk)
#     build_vacinacao(uf, df, municipio, n_chunk)
#     build_paciente(uf, df, n_chunk)
#     build_estabelecimento(uf, df,municipio, n_chunk)


#     n_chunk = n_chunk + 1

# os.system('rm  -r /tmp/data')

end = time.perf_counter()

print(f'Finished in {round(end-start,2)} seconds')