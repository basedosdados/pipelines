"""
Build the br_sfb_sicar dictionary parquet (all-string) from the downloaded
shapefiles: cod_tema -> nom_tema per overlay theme (column `tipo`), plus the
fixed status / property-type code labels. Written to OUTPUT/dicionario/.
"""

import glob
import os
from pathlib import Path

import architecture as A  # noqa: N812
import clean as C  # noqa: N812
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

SCRATCH = Path(
    os.environ.get(
        "CAR_DATA", str(Path.home() / "Downloads" / "br_sfb_sicar_data")
    )
)
INPUT = SCRATCH / "input"
OUTPUT = SCRATCH / "output"

POLY_TO_TABLE = {v: k for k, v in A.THEME_POLYGON.items()}

STATUS = {"AT": "Ativo", "PE": "Pendente", "SU": "Suspenso", "CA": "Cancelado"}
TIPO_IMOVEL = {
    "IRU": "Imóvel Rural",
    "AST": "Assentamento de Reforma Agrária",
    "PCT": "Povos e Comunidades Tradicionais",
}


def collect_tipo_labels():
    """Union of distinct (table, cod_tema, nom_tema) across downloaded zips."""
    pairs = {}  # (table, chave) -> valor
    for zf in sorted(glob.glob(str(INPUT / "*.zip"))):
        base = os.path.basename(zf).replace(".zip", "")
        _, poly = base.split("_", 1)
        table = POLY_TO_TABLE.get(poly)
        if table is None or table == "area_imovel":
            continue
        gdf = C.read_theme_zip(zf)
        if "cod_tema" in gdf and "nom_tema" in gdf:
            sub = gdf[["cod_tema", "nom_tema"]].dropna().drop_duplicates()
            for _, r in sub.iterrows():
                pairs[(table, str(r["cod_tema"]))] = str(r["nom_tema"])
        del gdf
    return pairs


def main():
    rows = []

    # tipo labels for area_imovel (ind_tipo)
    for k, v in TIPO_IMOVEL.items():
        rows.append(("area_imovel", "tipo", k, "", v))

    # tipo labels for overlays (cod_tema -> nom_tema)
    for (table, chave), valor in sorted(collect_tipo_labels().items()):
        rows.append((table, "tipo", chave, "", valor))

    # status labels for every theme table
    for table in A.TABLES:
        for k, v in STATUS.items():
            rows.append((table, "status", k, "", v))

    df = pd.DataFrame(
        rows,
        columns=[
            "id_tabela",
            "nome_coluna",
            "chave",
            "cobertura_temporal",
            "valor",
        ],
    )
    df = df.drop_duplicates()
    root = OUTPUT / "dicionario"
    if root.exists():
        import shutil

        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([(c, pa.string()) for c in df.columns])
    pq.write_table(
        pa.Table.from_pandas(df, schema=schema, preserve_index=False),
        str(root / "dicionario.parquet"),
        compression="snappy",
    )
    print(f"dicionario rows: {len(df)} -> {root}")
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
