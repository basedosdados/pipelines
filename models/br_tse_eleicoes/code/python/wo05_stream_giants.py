"""Work order 05 — extract→stream→evict for the three giant seção families.

Runs the streaming build (``sub/streaming_secao.py``) over the uniform zip
vintage in ``gate_b_rebuild.REAL_INPUT``, one ``(ano, uf)`` at a time, writing
per-partition parquet to ``config.STREAM_SECAO_ROOT`` (= ``WORK/output_python/
_stream``). Reuses ``gate_b_rebuild``'s extract/_per_uf/cleanup so extractions
land in the same ``WORK/input`` tree the builders read, and are deleted per
family-year to stay within 16 GB RAM / the free disk budget.

Resumable: a ``(family, ano)`` whose ``_done`` marker exists is skipped.
Sequential by design — never run concurrently with the phase-1 rebuild.

Run from ``code/python`` with the same WORK as the phase-1 rebuild::

    TSE_WORK=/path/to/work uv run python wo05_stream_giants.py [--family F] [--only-year Y]

``--family`` is one of ``resultados_secao`` / ``perfil_secao`` / ``all``.
"""

from __future__ import annotations

import shutil
import sys
import zipfile

import config

# gate_b_rebuild sets os.environ["TSE_DATA_DIR"]=WORK at import; import it first
# so config (imported below) resolves OUTPUT_PYTHON/STREAM_SECAO_ROOT under WORK.
import gate_b_rebuild as gb
from sub import results_section as rsec
from sub import streaming_secao as ss
from sub import voter_profile_section as vps

STREAM_ROOT = config.STREAM_SECAO_ROOT
DONE = STREAM_ROOT / ".done"

RESULTADOS_YEARS = list(range(1994, 2025, 2))
PERFIL_YEARS = list(range(2008, 2025, 2))


def _ensure_static() -> None:
    """rsec._load_mun_uf reads MUNICIPIO_DIR_CSV from WORK/input."""
    (gb.WORK / "input").mkdir(parents=True, exist_ok=True)
    src = gb.REAL_INPUT / "br_bd_diretorios_brasil_municipio.csv"
    dst = gb.WORK / "input" / "br_bd_diretorios_brasil_municipio.csv"
    if src.exists() and not dst.exists():
        shutil.copy(src, dst)


def _marker(family: str, ano: int):
    return DONE / f"{family}_{ano}"


def _prep_votacao_secao(ano: int):
    """Extract votacao_secao inputs, handling both TSE layouts: per-UF zips
    (1994-2022) and the single national zip (2024,
    ``votacao_secao_eleitoral_<ano>.zip`` with ``secao_input/
    votacao_secao_<ano>_<UF>.csv`` inside). Lays every UF out at the per-UF
    path the builder reads and returns the cleanup rel-paths."""
    fam_dir = gb.REAL_INPUT / "votacao_secao"
    if list(fam_dir.glob(f"votacao_secao_{ano}_*.zip")):
        return gb._per_uf("votacao_secao", ano)
    nat = fam_dir / f"votacao_secao_eleitoral_{ano}.zip"
    if not nat.exists():
        return None
    prefix = f"votacao_secao_{ano}_"
    cleanups: list[str] = []
    with zipfile.ZipFile(nat) as zf:
        for name in zf.namelist():
            base = name.rsplit("/", 1)[-1]
            if not (base.startswith(prefix) and base.endswith(".csv")):
                continue
            uf = base[len(prefix) : -len(".csv")]
            rel = f"votacao_secao/votacao_secao_{ano}_{uf}"
            dest = gb.WORK / "input" / rel
            dest.mkdir(parents=True, exist_ok=True)
            with zf.open(name) as src, open(dest / base, "wb") as out:
                shutil.copyfileobj(src, out, length=1 << 20)
            cleanups.append(rel)
    return cleanups or None


def _run_year(family: str, ano: int, prep, stream_fn) -> None:
    marker = _marker(family, ano)
    if marker.exists():
        print(f"  skip {family} {ano} (done marker present)", flush=True)
        return
    cleanups = prep()
    if not cleanups:
        print(f"  {family} {ano}: NO INPUT ZIPS — skipped", flush=True)
        return
    try:
        stream_fn(ano, STREAM_ROOT)
        DONE.mkdir(parents=True, exist_ok=True)
        marker.write_text("ok\n")
        print(f"  DONE {family} {ano}", flush=True)
    finally:
        for c in cleanups:
            gb.cleanup(c)


def stream_resultados(years=RESULTADOS_YEARS) -> None:
    for ano in years:
        if ano not in rsec.UFS:
            continue
        print(f"[resultados_secao] {ano}", flush=True)
        _run_year(
            "resultados_secao",
            ano,
            lambda a=ano: _prep_votacao_secao(a),
            ss.stream_resultados_secao,
        )


def stream_perfil(years=PERFIL_YEARS) -> None:
    for ano in years:
        if ano not in vps.UFS:
            continue
        print(f"[perfil_secao] {ano}", flush=True)
        _run_year(
            "perfil_secao",
            ano,
            lambda a=ano: gb._per_uf(
                "perfil_eleitorado_secao", a, "perfil_eleitor_secao"
            ),
            ss.stream_perfil_secao,
        )


def main() -> None:
    family = "all"
    only_year = None
    if "--family" in sys.argv:
        family = sys.argv[sys.argv.index("--family") + 1]
    if "--only-year" in sys.argv:
        only_year = int(sys.argv[sys.argv.index("--only-year") + 1])
    _ensure_static()
    print(f"STREAM_ROOT={STREAM_ROOT}", flush=True)
    ry = [only_year] if only_year else RESULTADOS_YEARS
    py = [only_year] if only_year else PERFIL_YEARS
    if family in ("all", "resultados_secao"):
        stream_resultados(ry)
    if family in ("all", "perfil_secao"):
        stream_perfil(py)
    print("STREAM GIANTS COMPLETE", flush=True)


if __name__ == "__main__":
    main()
