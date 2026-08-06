import os
import zipfile
from pathlib import Path

import requests

input = (
    Path("input")
    / "br_inep_indicadores_educacionais"
    / "fluxo_educacao_superior"
)
output = (
    Path("output")
    / "br_inep_indicadores_educacionais"
    / "fluxo_educacao_superior"
)


def main():
    urls = [
        "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/indicadores_fluxo_brasil_2010_2024.zip",
        "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/indicadores_fluxo_regiao_2010_2024.zip",
        "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/indicadores_fluxo_UF_2010_2024.zip",
    ]

    input.mkdir(exist_ok=True, parents=True)
    output.mkdir(exist_ok=True, parents=True)

    skip_download = False

    if not skip_download:
        for url in urls:
            print(url)
            for attempt in range(5):
                try:
                    response = requests.get(
                        url,
                        headers={"User-Agent": "Mozilla/5.0"},
                        verify=False,
                        timeout=120,
                    )
                    response.raise_for_status()
                    break
                except requests.exceptions.RequestException as e:
                    if attempt == 4:
                        raise
                    print(f"  retry {attempt + 1}/5 ({e})")
            with open(os.path.join(input, url.split("/")[-1]), "wb") as f:
                f.write(response.content)

        for file in os.listdir(input):
            if file.endswith(".zip"):
                with zipfile.ZipFile(os.path.join(input, file)) as z:
                    z.extractall(input)
                    os.remove(os.path.join(input, file))


if __name__ == "__main__":
    main()
