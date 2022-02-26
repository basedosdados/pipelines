"""
Flows for br_ibge_ipca
"""

from prefect import Flow
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.utils import upload_to_gcs, create_bd_table, create_header
from prefect.tasks.shell import ShellTask


create_dirs = ShellTask(helper_script="create_dirs.sh")
append_csv = ShellTask(helper_script="group_csv.sh")
UFS=["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", 
    "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR",
    "RJ", "RN", "RO", "RR", "SC", "SE", "RS", "TO", "SP"]
MUNICIPIO = bd.read_sql(
    """
                          SELECT *
                          FROM basedosdados.br_bd_diretorios_brasil.municipio
                          """,
    billing_project_id="basedosdados-dev",
)

municipio.to_csv("/tmp/data/br_ms_vacinacao_covid19/aux/municipio.csv", index=False)

with Flow("br_ms_vacinacao_covid19.microdados") as br_ms_vacinacao_covid19_microdados:
    FOLDER="microdados/"
    create_dirs()
    crawler(UFS, "multiprocess")
    append_csv()

for uf in ufs:
    filename = "input/" + uf + ".csv"
    print("Using raw files of {}.".format(uf))
    chunksize = 10 ** 6
    n_chunk = 1

    for df in pd.read_csv(filename,
                            sep=";",
                            dtype=object,
                            chunksize=chunksize):

        print("Cleaning state {}_{}.".format(uf, n_chunk))
        df.fillna('')
        build_microdados(uf, df, MUNICIPIO, n_chunk)

        n_chunk = n_chunk + 1

    filepath = build_microdados(uf, df, municipio, n_chunk)
    dataset_id = "br_ms_vacinacao_covid19"
    table_id = "microdados"


    wait_header_path = create_header(path=filepath)

    # Create table in BigQuery
    wait_create_bd_table = create_bd_table(  # pylint: disable=invalid-name
        path=wait_header_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_type="overwrite",
        wait=wait_header_path,
    )

    # Upload to GCS
    upload_to_gcs(
        path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        wait=wait_create_bd_table,
    )