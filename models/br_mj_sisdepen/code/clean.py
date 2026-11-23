"""Build the br_mj_sisdepen cleaned tables from the SISDEPEN cycle files.

    uv run models/br_mj_sisdepen/code/clean.py [--skip-download]

Raw downloads and cleaned parquet go under SISDEPEN_DATA_ROOT (default
~/Downloads/br_mj_sisdepen_data), never inside the repo or Dropbox.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

import models.br_mj_sisdepen.code.utils as utils
from models.br_mj_sisdepen.code.constants import (
    CYCLE_FILES,
    INPUT_DIR,
    OUTPUT_DIR,
    TABLES,
)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-download", action="store_true")
    ap.add_argument("--output-dir", default=str(OUTPUT_DIR))
    args = ap.parse_args()
    output_dir = Path(args.output_dir)

    # 1. acquire
    if args.skip_download:
        paths = {c: INPUT_DIR / Path(f).name for c, f in CYCLE_FILES.items()}
        missing = [c for c, p in paths.items() if not p.exists()]
        if missing:
            raise SystemExit(
                f"missing input for cycles {missing}; drop --skip-download"
            )
    else:
        paths = utils.download_all(INPUT_DIR)
    print(f"input: {len(paths)} cycles in {INPUT_DIR}")

    # 2. read and build the establishment panel
    raw, bases = {}, []
    for cycle in sorted(paths):
        df = utils.read_cycle(cycle, paths[cycle])
        base = utils.base_frame(df)
        agree = utils.check_capacity_reconciles(df, base)
        # block 1.3 publishes regime totals and sex margins over the same
        # quantity; if they disagree the capacity parse is wrong
        assert agree == 1.0, (
            f"cycle {cycle}: capacity reconciliation {agree:.4f}, expected 1.0"
        )
        raw[cycle] = df
        bases.append(base)
        print(
            f"  cycle {cycle:>2} {int(base.ano.iloc[0])}/{int(base.semestre.iloc[0])} "
            f"rows={len(base):>5} gen={base.geracao_esquema.iloc[0]} reconcile={agree:.0%}"
        )
    panel = pd.concat(bases, ignore_index=True)

    # 3. reconstruct establishment identity across cycles
    panel = utils.link_units(panel)
    amb = (panel.pareamento_ambiguo == True).sum()  # noqa: E712
    linked = panel.metodo_pareamento.isin(["consecutivo", "lacuna"]).sum()
    print(
        f"\ncrosswalk: {panel.id_unidade.nunique()} units | "
        f"{linked} links | {amb} ambiguous ({100 * amb / max(linked, 1):.2f}%) | "
        f"{(panel.metodo_pareamento == 'lacuna').sum()} gap bridges"
    )

    # 4. extract
    tables = {}
    tables["unidade_prisional"] = utils.extract_unidade_prisional(raw, panel)
    tables["populacao_prisional"] = utils.extract_populacao_prisional(
        raw, panel
    )
    tables["populacao_caracteristica"] = (
        utils.extract_populacao_caracteristica(raw, panel)
    )
    tables["uf_semestre"] = utils.build_uf_semestre(
        panel, tables["populacao_prisional"]
    )
    tables["unidade_crosswalk"] = utils.build_unidade_crosswalk(panel)
    tables["cobertura"] = utils.build_cobertura(
        panel, tables["populacao_caracteristica"]
    )
    tables["dicionario"] = utils.build_dicionario()

    # 5. validate
    print("\nvalidation")
    pop_component = tables["populacao_prisional"].quantidade.sum()
    pop_panel = panel.populacao_total.sum()
    assert abs(pop_component - pop_panel) < 1, (
        f"population mismatch: components {pop_component} vs panel {pop_panel}"
    )
    print(
        f"  population components reconcile with panel totals: {pop_component:,.0f}"
    )

    cap_unit = tables["unidade_prisional"].capacidade_total.sum()
    cap_panel = panel.capacidade_total.sum()
    assert abs(cap_unit - cap_panel) < 1, (
        f"capacity mismatch: {cap_unit} vs {cap_panel}"
    )
    print(
        f"  capacity by regime reconciles with panel totals: {cap_unit:,.0f}"
    )

    for name, df in tables.items():
        if name == "dicionario":
            continue
        assert df.notna().any(axis=1).all(), f"{name}: all-null row"
    grain = {
        "unidade_prisional": ["ano", "semestre", "id_unidade", "tipo_regime"],
        "populacao_prisional": [
            "ano",
            "semestre",
            "id_unidade",
            "situacao_processual",
            "regime",
            "esfera_justica",
            "sexo",
        ],
        "populacao_caracteristica": [
            "ano",
            "semestre",
            "id_unidade",
            "caracteristica",
            "categoria",
            "sexo",
        ],
        "uf_semestre": ["ano", "semestre", "sigla_uf"],
        "unidade_crosswalk": ["ano", "semestre", "id_unidade"],
        "cobertura": ["ano", "semestre", "sigla_uf"],
    }
    for name, keys in grain.items():
        dup = tables[name].duplicated(keys).sum()
        assert dup == 0, f"{name}: {dup} duplicate rows on grain {keys}"
        print(f"  {name:26s} grain unique over {len(keys)} keys")

    # 6. write
    print(f"\nwriting to {output_dir}")
    for name in TABLES:
        n = utils.write_partitioned(tables[name], name, output_dir)
        print(f"  {name:26s} {n:>9,} rows  {tables[name].shape[1]:>2} cols")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
