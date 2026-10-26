import argparse
import zipfile
from pathlib import Path

import basedosdados as bd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.utils.tasks import _upload_to_gcs

DATASET_ID = "br_bd_diretorios_brasil"
TABLE_ID = "cbo_2002"
FINAL_COLUMNS = [
    "cbo_2002",
    "descricao",
    "familia",
    "descricao_familia",
    "subgrupo",
    "descricao_subgrupo",
    "subgrupo_principal",
    "descricao_subgrupo_principal",
    "grande_grupo",
    "descricao_grande_grupo",
    "indicador_cbo_2002_ativa",
]

# Arquivos do zip "Estrutura CBO (CSV)" e quantos dígitos do código de
# ocupação identificam cada nível da hierarquia.
NIVEIS = {
    "familia": ("Familia", 4),
    "subgrupo": ("SubGrupo", 3),
    "subgrupo_principal": ("SubGrupo Principal", 2),
    "grande_grupo": ("Grande Grupo", 1),
}


def ler_estrutura(arquivos: zipfile.ZipFile, nome: str) -> pd.Series:
    """Lê um dos CSVs da estrutura como série código -> título."""
    with arquivos.open(f"CBO2002 - {nome}.csv") as fp:
        df = pd.read_csv(
            fp,
            sep=";",
            encoding="latin1",
            dtype=str,
            names=["codigo", "titulo"],
            header=0,
        )
    df["codigo"] = df["codigo"].str.strip()
    return df.set_index("codigo")["titulo"]


def build_oficial(zip_path: Path) -> pd.DataFrame:
    """Monta uma linha por ocupação com a hierarquia completa."""
    arquivos = zipfile.ZipFile(zip_path)
    ocupacao = ler_estrutura(arquivos, "Ocupacao")

    df = pd.DataFrame(
        {"cbo_2002": ocupacao.index, "descricao": ocupacao.to_numpy()}
    )
    for nivel, (nome, digitos) in NIVEIS.items():
        df[nivel] = df["cbo_2002"].str[:digitos]
        df[f"descricao_{nivel}"] = df[nivel].map(ler_estrutura(arquivos, nome))
    df["indicador_cbo_2002_ativa"] = "1"

    sem_descricao = df.filter(like="descricao").isna().sum()
    if sem_descricao.any():
        raise ValueError(
            f"Níveis sem descrição:\n{sem_descricao[sem_descricao > 0]}"
        )
    return df


def read_existing(billing_project_id: str) -> pd.DataFrame:
    """Lê a tabela atual em produção."""
    df = bd.read_sql(
        f"select * from `basedosdados.{DATASET_ID}.{TABLE_ID}`",
        billing_project_id=billing_project_id,
    )
    return df[FINAL_COLUMNS].astype("string")


def build_final(existing: pd.DataFrame, oficial: pd.DataFrame) -> pd.DataFrame:
    """Usa a estrutura oficial e mantém como inativos os códigos que saíram dela."""
    fora = existing[~existing["cbo_2002"].isin(oficial["cbo_2002"])].copy()
    fora["indicador_cbo_2002_ativa"] = "0"

    novos = oficial[~oficial["cbo_2002"].isin(existing["cbo_2002"])]
    desativados = fora[
        existing.set_index("cbo_2002")
        .loc[fora["cbo_2002"], "indicador_cbo_2002_ativa"]
        .to_numpy()
        == "1"
    ]
    print(f"Existentes: {len(existing)} | Oficial: {len(oficial)}")
    print(f"Códigos novos ({len(novos)}):")
    print(novos[["cbo_2002", "descricao"]].to_string(index=False))
    print(f"Códigos que passam a inativos ({len(desativados)}):")
    print(desativados[["cbo_2002", "descricao"]].to_string(index=False))

    final = pd.concat([oficial, fora], ignore_index=True)[FINAL_COLUMNS]
    if not final["cbo_2002"].is_unique:
        raise ValueError("Código de ocupação duplicado")
    return final.sort_values("cbo_2002", ignore_index=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "zip_path",
        type=Path,
        help=(
            "Zip 'Estrutura CBO (CSV)' baixado de "
            "https://cbo.mte.gov.br/cbosite/pages/downloads.jsf "
            "(o site pede captcha, então o download é manual)"
        ),
    )
    parser.add_argument("--billing-project-id", required=True)
    args = parser.parse_args()

    root_dir = Path(__file__).parent.parent.parent.parent
    output_dir = root_dir / "tmp" / DATASET_ID / TABLE_ID / "output"
    output_dir.mkdir(exist_ok=True, parents=True)

    existing = read_existing(args.billing_project_id)
    oficial = build_oficial(args.zip_path)
    final = build_final(existing, oficial)

    # Mesmo nome do arquivo já presente no staging, para que o upload o substitua.
    output_path = output_dir / "diretorio_cbo_2002.parquet"
    schema = pa.schema([(coluna, pa.string()) for coluna in FINAL_COLUMNS])
    pq.write_table(
        pa.Table.from_pandas(final, schema=schema, preserve_index=False),
        output_path,
    )
    print(f"Arquivo salvo em {output_path}")

    _upload_to_gcs(
        data_path=output_path,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        bucket_name="basedosdados-dev",
        source_format="parquet",
    )
