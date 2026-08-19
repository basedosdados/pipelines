"""Onboarding estático do br_bcb_ifdata — limpa as 105 competências.

Reaproveita as funções puras de `pipelines/datasets/br_bcb_ifdata/utils.py`,
as mesmas que o pipeline trimestral usa, em vez de duplicar a transformação.

Cada competência é baixada, decodificada e gravada em parquet uma por vez, e
o JSON bruto nunca vai para o disco — só o parquet de saída ocupa espaço.

Uso:
    uv run python models/br_bcb_ifdata/code/clean.py [--desde AAAAMM]

Saída (padrão `~/Downloads/br_bcb_ifdata_data/output`, sobrescrevível pela
variável de ambiente `IFDATA_OUTPUT`):

    output/instituicao/ano=<AAAA>/data_<AAAAMM>.parquet
    output/coluna/ano=<AAAA>/data_<AAAAMM>.parquet
    output/relatorio/ano=<AAAA>/data_<AAAAMM>.parquet
    output/dicionario/data.parquet
"""

from __future__ import annotations

import argparse
import collections
import os
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[3]))

from pipelines.datasets.br_bcb_ifdata.utils import (
    build_dicionario,
    build_municipio_crosswalk,
    clean_period,
    fetch_index,
)

DEFAULT_OUT = (
    pathlib.Path.home() / "Downloads" / "br_bcb_ifdata_data" / "output"
)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--desde", type=int, default=0, help="competência AAAAMM")
    ap.add_argument("--ate", type=int, default=999912)
    args = ap.parse_args()

    outdir = pathlib.Path(os.environ.get("IFDATA_OUTPUT") or DEFAULT_OUT)
    outdir.mkdir(parents=True, exist_ok=True)
    print(f"saída: {outdir}", flush=True)

    exact, squashed = build_municipio_crosswalk()
    print(f"crosswalk município: {len(exact)} chaves", flush=True)

    index = [e for e in fetch_index() if args.desde <= e["dt"] <= args.ate]
    print(f"competências a processar: {len(index)}", flush=True)

    totais: collections.Counter = collections.Counter()
    nao_resolvidos: collections.Counter = collections.Counter()
    sem_rubrica = 0
    falhas: list[tuple[int, str]] = []
    t0 = time.time()

    for n, entry in enumerate(index, 1):
        dt = entry["dt"]
        try:
            st = clean_period(entry, outdir, exact, squashed)
        except Exception as exc:
            falhas.append((dt, repr(exc)))
            print(f"  [{n}/{len(index)}] {dt} FALHOU: {exc!r}", flush=True)
            continue
        for k in ("instituicao", "coluna", "relatorio"):
            totais[k] += st[k]
        nao_resolvidos.update(st["municipios_nao_resolvidos"])
        sem_rubrica += st["celulas_sem_rubrica"]
        print(
            f"  [{n}/{len(index)}] {dt}: inst={st['instituicao']:>5} "
            f"col={st['coluna']:>4} fato={st['relatorio']:>9} "
            f"({time.time() - t0:.0f}s)",
            flush=True,
        )

    print("\nmontando dicionário...", flush=True)
    dic = build_dicionario(index, outdir)

    print("\n=== TOTAIS ===")
    for k in ("instituicao", "coluna", "relatorio"):
        print(f"  {k:<12} {totais[k]:>12,}")
    print(f"  {'dicionario':<12} {dic['dicionario']:>12,}")
    print(f"\ncélulas sem rubrica: {sem_rubrica}")
    print(f"municípios não resolvidos: {sum(nao_resolvidos.values())}")
    for k, v in nao_resolvidos.most_common(30):
        print("  MISS", k, v)
    if falhas:
        print(f"\nFALHAS ({len(falhas)}):")
        for dt, err in falhas:
            print("  ", dt, err)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
