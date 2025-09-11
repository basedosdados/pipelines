from io import BytesIO
from pathlib import Path

import basedosdados as bd
import pandas as pd
import requests

CURRENT_DIR = Path(__file__).parent
PARENT_DIR = CURRENT_DIR.parent
TMP_DIR = PARENT_DIR / "tmp"
TMP_DIR.mkdir(exist_ok=True, parents=True)


def download_data(url: str, input_path: Path | str):
    print(f"Downloading {url}")

    response = requests.get(url, verify=False)
    if response.status_code == 200:
        data = BytesIO(response.content)
        df = pd.read_csv(data, encoding="latin-1", sep=";", dtype=str)
        df.to_csv(
            str(input_path),
            index=False,
            encoding="utf-8",
            sep=",",
        )
    else:
        print(f"Erro! O status da requisição foi {response.status_code}")


def upload_table_to_bigquery(
    table_id: str, path_to_data: str, dataset_id: str = f"{PARENT_DIR.stem}"
):
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)

    tb.create(
        path=path_to_data,
        if_storage_data_exists="replace",
        if_table_exists="replace",
        source_format="csv",
    )


def upload_data_to_big_query(output_dir: Path | str, table_ids: dict):
    try:
        for key in table_ids:
            try:
                upload_table_to_bigquery(
                    table_id=table_ids[key],
                    path_to_data=str(
                        Path(output_dir) / f"{table_ids[key]}.csv"
                    ),
                )
                # (Path(output_dir) / f"{table_ids[key]}.csv").unlink()
            except Exception as e:
                print(f"Unable to upload table to BiQuery: {e}")
        # shutil.rmtree(Path(output_dir))
    except Exception as e:
        print(f"Unable to upload table to BiQuery: {e}")
