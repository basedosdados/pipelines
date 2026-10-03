"""Municipality and espécie coverage report, by year.

The source identifies a município only by name — its ``Mun Resid`` digits are an
INSS Gerência-Executiva, not a municipal code — so the join to
``br_bd_diretorios_brasil.municipio`` is by name and can silently lose rows.
This report makes that loss explicit per year: how many municípios in the
directory were seen, how many rows carried no município at all, and which names
failed to resolve.

Reads the staged per-competência parquet and the diagnostics written by
concedido_clean.py / mantido_clean.py; writes Markdown to
``<scratch>/reports/coverage_<table>.md``.

Usage:
    uv run python -m models.br_mps_beneficios.code.coverage_report [--table TABLE]
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from collections import defaultdict
from pathlib import Path

import pandas as pd

from pipelines.datasets.br_mps_beneficios import utils as u

BASE = Path(
    os.environ.get(
        "BR_MPS_BENEFICIOS_DATA",
        Path.home() / "Library/Caches/br_mps_beneficios_data",
    )
)


def directory_municipios() -> dict[str, str]:
    """id_municipio -> "UF Nome", for naming the gaps rather than just counting."""
    with open(u.constants.MUNICIPIO_DIRECTORY.value, encoding="utf-8") as fh:
        return {
            r["id_municipio"]: f"{r['sigla_uf']} {r['nome']}"
            for r in csv.DictReader(fh)
        }


def build(table: str) -> str:
    staging = BASE / "staging" / table
    files = sorted(staging.glob("comp=*.parquet"))
    if not files:
        raise SystemExit(f"nothing staged for {table} at {staging}")
    diag_path = (
        BASE
        / "reports"
        / (
            "concedido_diagnostics.json"
            if "concedido" in table
            else "mantido_diagnostics.json"
        )
    )
    diags = json.loads(diag_path.read_text()) if diag_path.exists() else []

    directory = directory_municipios()
    per_year: dict[int, dict] = defaultdict(
        lambda: {
            "competencias": set(),
            "municipios": set(),
            "quantidade": 0,
            "quantidade_sem_municipio": 0,
            "especies": set(),
            "categorias": set(),
        }
    )
    for f in files:
        comp = int(f.stem.split("=")[1])
        df = pd.read_parquet(f)
        y = per_year[comp // 100]
        y["competencias"].add(comp)
        y["municipios"].update(df["id_municipio"].dropna().unique().tolist())
        y["quantidade"] += int(df["quantidade"].sum())
        y["quantidade_sem_municipio"] += int(
            df.loc[df["id_municipio"].isna(), "quantidade"].sum()
        )
        y["especies"].update(
            df["especie_beneficio"].dropna().unique().tolist()
        )
        y["categorias"].update(
            df["categoria_beneficio"].dropna().unique().tolist()
        )

    # names the crosswalk did not resolve, aggregated across the run
    unresolved: dict[str, int] = defaultdict(int)
    for d in diags:
        for k, v in (d.get("nomes_nao_encontrados") or {}).items():
            unresolved[k] += v
        for k, v in (d.get("chaves_municipio_nao_encontradas") or {}).items():
            unresolved[k] += v
    errors = [d for d in diags if d.get("erro")]

    total_mun = len(directory)
    lines = [
        f"# Cobertura de municípios e espécies — `{table}`",
        "",
        f"Diretório de referência: {total_mun} municípios "
        "(`br_bd_diretorios_brasil.municipio`).",
        "",
        "O campo `Mun Resid` da fonte não traz código de município — seus dígitos",
        "são o código da Gerência-Executiva do INSS — de modo que a resolução é",
        "feita por nome. A tabela abaixo quantifica a perda por ano.",
        "",
        "| ano | meses | municípios | % do diretório | ausentes | benefícios | "
        "sem município | % sem município | espécies | categorias |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for ano in sorted(per_year):
        y = per_year[ano]
        n = len(y["municipios"])
        pct = 100 * n / total_mun
        semp = 100 * y["quantidade_sem_municipio"] / max(y["quantidade"], 1)
        lines.append(
            f"| {ano} | {len(y['competencias'])} | {n} | {pct:.2f}% | "
            f"{total_mun - n} | {y['quantidade']:,} | "
            f"{y['quantidade_sem_municipio']:,} | {semp:.2f}% | "
            f"{len(y['especies'])} | {len(y['categorias'])} |"
        )

    seen_all = set().union(*(y["municipios"] for y in per_year.values()))
    never = set(directory) - seen_all
    named = sorted(f"{directory[i]} ({i})" for i in never)
    lines += [
        "",
        f"Municípios do diretório nunca observados em toda a série: **{len(never)}**"
        + (f" — {', '.join(named)}" if 0 < len(never) <= 40 else ""),
        "",
        "Nenhum deles é uma falha de correspondência: a fonte nunca publica "
        "esses nomes em nenhuma competência.",
        "",
        "## Nomes não resolvidos",
        "",
    ]
    if unresolved:
        lines.append("| chave da fonte | linhas |")
        lines.append("|---|---|")
        lines += [
            f"| `{k}` | {v:,} |"
            for k, v in sorted(unresolved.items(), key=lambda x: -x[1])[:60]
        ]
        lines.append("")
        lines.append(
            f"Total: {len(unresolved)} chaves distintas, "
            f"{sum(unresolved.values()):,} linhas. Cada uma precisa de uma entrada "
            "em `models/br_mps_beneficios/code/municipio_crosswalk.csv`."
        )
    else:
        lines.append(
            "Nenhum. Todos os nomes de município publicados pela fonte foram "
            "resolvidos para um código IBGE."
        )
    if errors:
        lines += ["", "## Recursos que falharam", ""]
        lines += [
            f"- `{d.get('recurso') or d.get('competencia')}`: {d['erro']}"
            for d in errors
        ]
    return "\n".join(lines) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", default="beneficio_concedido_municipio_mes")
    args = ap.parse_args()
    text = build(args.table)
    out = BASE / "reports" / f"coverage_{args.table}.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text, encoding="utf-8")
    print(text)
    print(f"written to {out}")


if __name__ == "__main__":
    main()
