"""Atualiza a tabela `br_bd_diretorios_brasil.municipio` com a API de
Localidades do IBGE.

- acrescenta municípios que existem no IBGE e não existem na tabela, com as
  colunas que a API fornece (ids e nomes de UF, região, mesorregião,
  microrregião, região imediata e região intermediária);
- NÃO altera nenhuma outra coluna dos municípios existentes. Ids que a API
  não fornece (`id_municipio_tse`, `id_municipio_bcb`, `id_municipio_rf`,
  `id_comarca`, `id_regiao_saude`, `centroide`, ...) continuam como estão.
"""

import time
from pathlib import Path

import basedosdados as bd
import pandas as pd
import requests

DATASET_ID = "br_bd_diretorios_brasil"
TABLE_ID = "municipio"
BILLING_PROJECT_ID = "basedosdados-dev"

IBGE_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"

COLUMNS_IBGE = [
    "id_municipio",
    "id_municipio_6",
    "nome",
    "id_regiao_imediata",
    "nome_regiao_imediata",
    "id_regiao_intermediaria",
    "nome_regiao_intermediaria",
    "id_microrregiao",
    "nome_microrregiao",
    "id_mesorregiao",
    "nome_mesorregiao",
    "id_uf",
    "sigla_uf",
    "nome_uf",
    "nome_regiao",
]


def get_key_val(d: dict | None, *path):
    """Percorre dicionários aninhados; devolve None se faltar algum nível."""
    for key in path:
        if not isinstance(d, dict):
            return None
        d = d.get(key)
    return d


def download_ibge(retries: int = 3, timeout: int = 60) -> pd.DataFrame:
    """Baixa os municípios da API do IBGE e devolve com o esquema da BD."""
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(IBGE_URL, timeout=timeout)
            response.raise_for_status()
            break
        except requests.RequestException:
            if attempt == retries:
                raise
            time.sleep(2 * attempt)

    rows = []
    # pyrefly: ignore [unbound-name]
    for item in response.json():
        # Alguns municípios vêm sem microrregião/mesorregião; a UF e a região
        # podem então ser lidas pela região imediata.
        uf = get_key_val(
            item, "microrregiao", "mesorregiao", "UF"
        ) or get_key_val(item, "regiao-imediata", "regiao-intermediaria", "UF")
        rows.append(
            {
                "id_municipio": str(item["id"]),
                "id_municipio_6": str(item["id"])[:6],
                "nome": item["nome"],
                "id_regiao_imediata": get_key_val(
                    item, "regiao-imediata", "id"
                ),
                "nome_regiao_imediata": get_key_val(
                    item, "regiao-imediata", "nome"
                ),
                "id_regiao_intermediaria": get_key_val(
                    item, "regiao-imediata", "regiao-intermediaria", "id"
                ),
                "nome_regiao_intermediaria": get_key_val(
                    item, "regiao-imediata", "regiao-intermediaria", "nome"
                ),
                "id_microrregiao": get_key_val(item, "microrregiao", "id"),
                "nome_microrregiao": get_key_val(item, "microrregiao", "nome"),
                "id_mesorregiao": get_key_val(
                    item, "microrregiao", "mesorregiao", "id"
                ),
                "nome_mesorregiao": get_key_val(
                    item, "microrregiao", "mesorregiao", "nome"
                ),
                "id_uf": get_key_val(uf, "id"),
                "sigla_uf": get_key_val(uf, "sigla"),
                "nome_uf": get_key_val(uf, "nome"),
                "nome_regiao": get_key_val(uf, "regiao", "nome"),
            }
        )

    df = pd.DataFrame(rows, columns=COLUMNS_IBGE)
    for col in df.columns:
        df[col] = df[col].map(lambda v: None if pd.isna(v) else str(v))
    return df.drop_duplicates(subset=["id_municipio"])


def read_existing(billing_project_id: str) -> pd.DataFrame:
    """Lê a tabela atual em produção, tudo como string."""
    df = bd.read_sql(
        f"select * from `basedosdados.{DATASET_ID}.{TABLE_ID}`",
        billing_project_id=billing_project_id,
    )
    df["id_municipio"] = df["id_municipio"].astype(str).str.strip()
    return df


def build_final(
    existing: pd.DataFrame,
    ibge: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Atualiza `nome` e acrescenta municípios novos. Nada mais é alterado.

    Devolve a tabela final e o dicionário de nomes antigos e novos.
    """
    df_final = existing.copy()
    nome_ibge = ibge.set_index("id_municipio")["nome"]

    ids_existentes = df_final["id_municipio"]
    novo_nome = ids_existentes.map(nome_ibge)
    mudou = novo_nome.notna() & (novo_nome != df_final["nome"])

    diff = pd.DataFrame(
        {
            "id_municipio": ids_existentes[mudou],
            "sigla_uf": df_final.loc[mudou, "sigla_uf"],
            "nome_antigo": df_final.loc[mudou, "nome"],
            "nome_novo": novo_nome[mudou],
        }
    ).sort_values(["sigla_uf", "id_municipio"])
    if not diff.empty:
        print(diff.to_string(index=False))

    df_final.loc[mudou, "nome"] = novo_nome[mudou]

    novos = ibge[~ibge["id_municipio"].isin(df_final["id_municipio"])]
    if not novos.empty:
        print(f"Municípios novos no IBGE ({len(novos)}):")
        print(
            novos[["id_municipio", "sigla_uf", "nome"]].to_string(index=False)
        )
        df_final = pd.concat([df_final, novos], ignore_index=True)[
            list(existing.columns)
        ]

    ausentes = existing[~existing["id_municipio"].isin(ibge["id_municipio"])]
    if not ausentes.empty:
        print(
            f"Aviso: {len(ausentes)} município(s) da BD não estão no IBGE "
            "(mantidos):"
        )
        print(
            ausentes[["id_municipio", "sigla_uf", "nome"]].to_string(
                index=False
            )
        )

    sem_ids = df_final[
        df_final["id_municipio_tse"].isna()
        | df_final["id_municipio_bcb"].isna()
    ]
    if not sem_ids.empty:
        print(
            f"Pendente: {len(sem_ids)} município(s) sem id_municipio_tse ou "
            "id_municipio_bcb (preencher manualmente):"
        )
        print(
            sem_ids[
                [
                    "id_municipio",
                    "sigla_uf",
                    "nome",
                    "id_municipio_tse",
                    "id_municipio_bcb",
                ]
            ].to_string(index=False)
        )

    return df_final.sort_values("id_municipio").reset_index(drop=True), diff


if __name__ == "__main__":
    billing_project_id = BILLING_PROJECT_ID
    upload = False

    root_dir = Path(__file__).parent.parent.parent.parent
    output_dir = root_dir / "tmp" / DATASET_ID / TABLE_ID / "output"
    output_dir.mkdir(exist_ok=True, parents=True)

    existing = read_existing(billing_project_id)
    ibge = download_ibge()
    df_final, dicionario = build_final(existing, ibge)

    output_path = output_dir / "municipio.csv"
    df_final.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Arquivo salvo em {output_path}")

    dicionario_path = output_dir / "municipio_nomes_antigos_novos.csv"
    dicionario.to_csv(dicionario_path, index=False, encoding="utf-8")
    print(f"Dicionário de nomes salvo em {dicionario_path}")

    if upload:
        from pipelines.utils.tasks import _upload_to_gcs

        _upload_to_gcs(
            data_path=output_path,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            bucket_name="basedosdados-dev",
        )
