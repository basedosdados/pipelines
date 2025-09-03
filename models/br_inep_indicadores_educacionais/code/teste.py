import os
import sys
import zipfile

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(
    current_dir
)  # Isso vai para /home/laribrito/BD/pipelines/models/br_inep_indicadores_educacionais/
sys.path.append(parent_dir)

# Diretórios base
INPUT = os.path.join(parent_dir, "tmp")
OUTPUT = os.path.join(parent_dir, "output")
INPUT_BR = os.path.join(INPUT, "br_regioes_uf")

# Garante que os diretórios existam
os.makedirs(INPUT, exist_ok=True)
os.makedirs(INPUT_BR, exist_ok=True)
os.makedirs(OUTPUT, exist_ok=True)

# Loop pelos arquivos em INPUT_BR
for file in os.listdir(INPUT_BR):
    path = os.path.join(INPUT_BR, file)
    if os.path.isfile(path) and file.lower().endswith(".zip"):
        try:
            with zipfile.ZipFile(path) as z:
                z.extractall(INPUT_BR)
            print(f"Extraído: {file}")
        except zipfile.BadZipFile:
            print(f"Arquivo inválido (não é zip de verdade): {file}")
