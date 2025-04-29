# -*- coding: utf-8 -*-
import os
import basedosdados as bd
import pandas as pd
from pathlib import Path
import zipfile
from utils import (
    RENAMES_BR,
    get_nivel_serie_disciplina,
    get_disciplina_serie,
    convert_to_pd_dtype,
    drop_empty_lines,
)

CWD = os.getcwd()

INPUT = os.path.join(CWD, "input")
OUTPUT = os.path.join(CWD, "output")

os.makedirs(INPUT, exist_ok=True)
os.makedirs(OUTPUT, exist_ok=True)

URL_ZIP = "https://download.inep.gov.br/microdados/microdados_saeb_2023.zip"

os.chdir(INPUT)
download = os.system(f"curl -O -k {URL_ZIP}")
os.chdir(CWD)
assert download == 0

with zipfile.ZipFile(Path(INPUT) / "microdados_saeb_2023.zip") as z:
    z.extractall(INPUT)

df = pd.read_csv(Path("input") / "MICRODADOS_SAEB_2023" / "DADOS" / "TS_ALUNO_2EF.csv")
