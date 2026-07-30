"""Gate B — phase-1 rebuild from the local input zips (fix_plan/04-05).

Rebuilds every phase-1 table x year from the zip vintage in
``~/Downloads/dados_TSE/input`` (one uniform vintage — decision 2), into
``TSE_WORK/output_python``. After each build the new parquet is compared
against the March reference parquet; identical cells are logged MATCH,
divergent cells are logged with a row count so they can be reconciled
against gate_a_triage.md (expected: only the stale 2014/2022 secao
cells and the two blank-record drops).

Resumable: a table-year whose parquet already exists in the rebuild
output dir is skipped. Extractions are deleted per family-year.
Sequential by design — never parallelize.

Run from ``code/python/`` (TSE_WORK defaults under /tmp scratch):

    TSE_WORK=/path/to/work uv run python gate_b_rebuild.py [--family F]
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

REAL_INPUT = Path("/Users/rdahis/Downloads/dados_TSE/input")
MARCH_OUT = Path("/Users/rdahis/Downloads/dados_TSE/output_python")
WORK = Path(os.environ.get("TSE_WORK", "/tmp/tse_gate_b"))
os.environ["TSE_DATA_DIR"] = str(WORK)

sys.path.insert(0, str(Path(__file__).resolve().parent))

LOG = (
    Path(__file__).resolve().parent.parent
    / "fix_plan"
    / "gate_b_rebuild_log.md"
)


def log(msg: str) -> None:
    with open(LOG, "a") as fh:
        fh.write(msg + "\n")
    print(msg, flush=True)


def extract(rel_zip: str, dest_rel: str, members: str | None = None) -> bool:
    """Extract one zip from the real input tree into the work input tree."""
    src = REAL_INPUT / rel_zip
    dest = WORK / "input" / dest_rel
    if not src.exists():
        return False
    dest.mkdir(parents=True, exist_ok=True)
    try:
        with zipfile.ZipFile(src) as zf:
            names = zf.namelist()
            if members:
                names = [n for n in names if members in n]
            zf.extractall(dest, members=names)
        return True
    except zipfile.BadZipFile:
        # the 2006 prestacao "zip" is a RAR — libarchive tar reads it
        cmd = ["tar", "-xf", str(src), "-C", str(dest)]
        if members:
            cmd += ["--include", f"*{members}*"]
        r = subprocess.run(cmd, capture_output=True)
        if r.returncode == 0:
            # RAR carries a wrapping top dir; flatten it if present
            inner = list(dest.iterdir())
            if (
                len(inner) == 1
                and inner[0].is_dir()
                and inner[0].name == dest.name
            ):
                for p in inner[0].iterdir():
                    shutil.move(str(p), dest)
                inner[0].rmdir()
            return True
        return False


def cleanup(dest_rel: str) -> None:
    d = WORK / "input" / dest_rel
    if d.exists():
        shutil.rmtree(d)


def compare(stem: str) -> str:
    import gc

    import pandas as pd

    new_p = WORK / "output_python" / f"{stem}.parquet"
    old_p = MARCH_OUT / f"{stem}.parquet"
    if not old_p.exists():
        return "NO_MARCH_REF"
    nr = pd.read_parquet(new_p)
    old = pd.read_parquet(old_p)
    eq = nr.equals(old)
    res = "MATCH" if eq else f"DIFF rows {len(nr)} vs {len(old)}"
    del nr, old
    gc.collect()
    return res


def build_years(
    module: str, fn: str, years, stem_tpl: str, prep, ret_pair=False
):
    """Generic per-year loop: prep(ano) extracts, builder runs, compare, clean."""
    import gc
    import importlib

    mod = importlib.import_module(module)
    for ano in years:
        stems = (
            [stem_tpl.format(ano=ano)]
            if not ret_pair
            else [t.format(ano=ano) for t in stem_tpl]
        )
        outs = [WORK / "output_python" / f"{s}.parquet" for s in stems]
        if all(o.exists() for o in outs):
            continue
        cleanups = prep(ano)
        if cleanups is None:
            log(f"| {stems[0]} | SKIP | missing input zip |")
            continue
        try:
            result = getattr(mod, fn)(ano)
            (WORK / "output_python").mkdir(parents=True, exist_ok=True)
            if ret_pair:
                for df, s in zip(result, stems, strict=True):
                    df.to_parquet(
                        WORK / "output_python" / f"{s}.parquet", index=False
                    )
                del result
            else:
                result.to_parquet(outs[0], index=False)
                del result
            gc.collect()
            for s in stems:
                log(f"| {s} | {compare(s)} | |")
        except Exception as e:
            log(f"| {stems[0]} | ERROR | {type(e).__name__}: {str(e)[:120]} |")
        finally:
            for c in cleanups:
                cleanup(c)
            gc.collect()


YEARS_EVEN = list(range(1994, 2025, 2))

_PRESTACAO_ZIPS = {
    2002: [("prestacao_contas_2002.zip", "prestacao_contas_2002", None)],
    2004: [
        ("prestacao_contas_2004.zip", "prestacao_contas_2004", "Candidato")
    ],
    2006: [
        ("prestacao_contas_2006.zip", "prestacao_contas_2006", "Candidato")
    ],
    2008: [("prestacao_contas_2008.zip", "prestacao_contas_2008", None)],
    2010: [("prestacao_contas_2010.zip", "prestacao_contas_2010", None)],
    2012: [("prestacao_final_2012.zip", "prestacao_final_2012", None)],
    2014: [
        ("prestacao_final_2014.zip", "prestacao_final_2014", None),
        (
            "prestacao_contas_final_sup_2014.zip",
            "prestacao_contas_final_sup_2014",
            None,
        ),
    ],
    2016: [
        ("prestacao_contas_2016.zip", "prestacao_contas_2016", None),
        (
            "prestacao_contas_final_2016.zip",
            "prestacao_contas_final_2016",
            None,
        ),
        (
            "prestacao_contas_final_sup_2016.zip",
            "prestacao_contas_final_sup_2016",
            None,
        ),
    ],
}
for _y in (2018, 2020, 2022, 2024):
    _PRESTACAO_ZIPS[_y] = [
        (
            f"prestacao_de_contas_eleitorais_candidatos_{_y}.zip",
            f"prestacao_de_contas_eleitorais_candidatos_{_y}",
            None,
        )
    ]


def _prep_prestacao(ano: int) -> list[str] | None:
    outs = []
    for zname, drel, members in _PRESTACAO_ZIPS.get(ano, []):
        rel = f"prestacao_contas/{drel}"
        if extract(f"prestacao_contas/{zname}", rel, members=members):
            outs.append(rel)
    return outs or None


def _single(family: str, ano: int) -> list[str] | None:
    rel = f"{family}/{family}_{ano}"
    return [rel] if extract(rel + ".zip", rel) else None


def _per_uf(
    family: str, ano: int, prefix: str | None = None
) -> list[str] | None:
    prefix = prefix or family
    zips = sorted((REAL_INPUT / family).glob(f"{prefix}_{ano}_*.zip"))
    if not zips:
        return None
    outs = []
    for z in zips:
        rel = f"{family}/{z.stem}"
        if extract(f"{family}/{z.name}", rel):
            outs.append(rel)
    return outs or None


FAMILIES: dict[str, dict] = {
    "candidatos": dict(
        module="sub.candidates",
        fn="build_candidatos",
        years=YEARS_EVEN,
        stem="candidatos_{ano}",
        prep=lambda a: (
            (
                (_single("consulta_cand", a) or [])
                + (
                    _single_named(
                        "consulta_cand", f"consulta_cand_complementar_{a}"
                    )
                    or []
                )
            )
            or None
        ),
    ),
    "partidos": dict(
        module="sub.parties",
        fn="build_partidos",
        years=[1990, *YEARS_EVEN],
        stem="partidos_{ano}",
        prep=lambda a: (
            _single("consulta_coligacao", a)
            or _single_named("consulta_coligacao", f"consulta_legendas_{a}")
        ),
    ),
    "vagas": dict(
        module="sub.vacancies",
        fn="build_vagas",
        years=YEARS_EVEN,
        stem="vagas_{ano}",
        prep=lambda a: _single("consulta_vagas", a),
    ),
    "bens": dict(
        module="sub.campaign_finance",
        fn="build_bens",
        years=list(range(2006, 2025, 2)),
        stem="bens_candidato_{ano}",
        prep=lambda a: _single("bem_candidato", a),
    ),
    "detalhes_mun_zona": dict(
        module="sub.voting_details_mun_zone",
        fn="build_detalhes_mun_zona",
        years=YEARS_EVEN,
        stem="detalhes_votacao_municipio_zona_{ano}",
        prep=lambda a: _single("detalhe_votacao_munzona", a),
    ),
    "detalhes_secao": dict(
        module="sub.voting_details_section",
        fn="build_detalhes_secao",
        years=list(range(1998, 2025, 2)),
        stem="detalhes_votacao_secao_{ano}",
        prep=lambda a: _single("detalhe_votacao_secao", a),
    ),
    "perfil_mun_zona": dict(
        module="sub.voter_profile_mun_zone",
        fn="build_perfil_mun_zona",
        years=YEARS_EVEN,
        stem="perfil_eleitorado_municipio_zona_{ano}",
        prep=lambda a: _single("perfil_eleitorado", a),
    ),
    "perfil_secao": dict(
        module="sub.voter_profile_section",
        fn="build_perfil_secao",
        years=list(range(2008, 2025, 2)),
        stem="perfil_eleitorado_secao_{ano}",
        prep=lambda a: _per_uf(
            "perfil_eleitorado_secao", a, prefix="perfil_eleitor_secao"
        ),
    ),
    "local_votacao": dict(
        module="sub.voter_profile_polling_place",
        fn="build_perfil_local_votacao",
        years=list(range(2010, 2025, 2)),
        stem="perfil_eleitorado_local_votacao_{ano}",
        prep=lambda a: (
            _single("perfil_eleitorado_local_votacao", a)
            or _single_named(
                "perfil_eleitorado_local_votacao",
                f"eleitorado_local_votacao_{a}",
            )
        ),
    ),
    "rcmz": dict(
        module="sub.results_mun_zone",
        fn="build_candidato",
        years=YEARS_EVEN,
        stem="resultados_candidato_municipio_zona_{ano}",
        prep=lambda a: _single("votacao_candidato_munzona", a),
    ),
    "rpmz": dict(
        module="sub.results_mun_zone",
        fn="build_partido",
        years=YEARS_EVEN,
        stem="resultados_partido_municipio_zona_{ano}",
        prep=lambda a: _single("votacao_partido_munzona", a),
    ),
    "receitas": dict(
        module="sub.campaign_finance",
        fn="build_receitas",
        years=list(range(2002, 2025, 2)),
        stem="receitas_candidato_{ano}",
        prep=_prep_prestacao,
    ),
    "despesas": dict(
        module="sub.campaign_finance",
        fn="build_despesas",
        years=list(range(2002, 2025, 2)),
        stem="despesas_candidato_{ano}",
        prep=_prep_prestacao,
    ),
    "resultados_secao": dict(
        module="sub.results_section",
        fn="build_resultados_secao",
        years=YEARS_EVEN,
        stem=(
            "resultados_candidato_secao_{ano}",
            "resultados_partido_secao_{ano}",
        ),
        ret_pair=True,
        prep=lambda a: _per_uf("votacao_secao", a),
    ),
}


def _single_named(family: str, stem: str) -> list[str] | None:
    rel = f"{family}/{stem}"
    return [rel] if extract(rel + ".zip", rel) else None


def main() -> None:
    only = None
    if "--family" in sys.argv:
        only = sys.argv[sys.argv.index("--family") + 1]
    (WORK / "input").mkdir(parents=True, exist_ok=True)
    # static support files
    for hist in (
        "votacao_candidato_uf",
        "votacao_partido_uf",
        "detalhe_votacao_uf",
    ):
        src = REAL_INPUT / hist
        dst = WORK / "input" / hist
        if src.exists() and not dst.exists():
            dst.symlink_to(src)
    for f in ("br_bd_diretorios_brasil_municipio.csv", "geolocal_11out.csv"):
        src = REAL_INPUT / f
        dst = WORK / "input" / f
        if src.exists() and not dst.exists():
            shutil.copy(src, dst)
    if not LOG.exists():
        LOG.write_text(
            "# Gate B — phase-1 rebuild log (zip vintage vs March parquets)\n\n"
            "| table-year | result | note |\n|---|---|---|\n"
        )
    for name, spec in FAMILIES.items():
        if only and only != name:
            continue
        log(f"| — {name} — | | |")
        build_years(
            spec["module"],
            spec["fn"],
            spec["years"],
            spec["stem"],
            spec["prep"],
            ret_pair=spec.get("ret_pair", False),
        )


if __name__ == "__main__":
    main()
