import os
import zipfile
from pathlib import Path

import pandas as pd
import requests

CWD = Path("models") / "br_inep_saeb"
INPUT = CWD / "input"
OUTPUT = CWD / "output"

os.makedirs(INPUT, exist_ok=True)
os.makedirs(OUTPUT, exist_ok=True)

URL = "https://download.inep.gov.br/microdados/microdados_saeb_2023.zip"

r = requests.get(
    URL, headers={"User-Agent": "Mozilla/5.0"}, verify=False, stream=True
)

with open(INPUT / "microdados_saeb_2023.zip", "wb") as fd:
    for chunk in r.iter_content(chunk_size=128):
        fd.write(chunk)

with zipfile.ZipFile(INPUT / "microdados_saeb_2023.zip") as z:
    z.extractall(INPUT)


df_aluno_ef_2ano = pd.read_csv(
    INPUT / "MICRODADOS_SAEB_2023" / "DADOS" / "TS_ALUNO_2EF.csv", sep=";"
)
