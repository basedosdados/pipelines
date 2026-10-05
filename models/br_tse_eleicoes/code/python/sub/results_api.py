"""
Stand-in for the CDN files ``votacao_partido_munzona_{ano}`` and
``detalhe_votacao_munzona_{ano}``, rebuilt from the TSE results-divulgation API
(``resultados.tse.jus.br``) while the CDN does not yet publish them.

The API serves one "unified results" JSON per (eleição, município, zona,
cargo) — document EA20 in TSE's "Instruções para download dos arquivos da
divulgação". This module downloads every zone file of an election, then writes
CSVs in the **same layout as the official CDN files** (same header, ``;``
separator, latin-1), under the same INPUT_DIR paths. The existing builders
(``voting_details_mun_zone``, ``results_mun_zone``) therefore read them
unchanged, and switching back to the official files is just deleting these
and running ``download.py`` for the two families.

What the API does not carry:

- ``QT_SECOES_AGREGADAS`` — derived from ``eleitorado_local_votacao_{ano}``
  (sections typed "Agregada" per município/zona). That file is a pre-election
  snapshot, so later aggregations are missed.
- the split of annulled votes into nominal/legenda (``QT_VOTOS_*_ANULADOS``)
  and ``ST_VOTO_EM_TRANSITO`` detail — written as ``-1`` / ``"N"``. No
  builder reads them.

Validated against the official 2024 files (eleição 619): see
``validate_against_official``.

Usage:
    TSE_DATA_DIR=... python -m models.br_tse_eleicoes.code.python.sub.results_api 2026 6257 6259
"""

import gzip
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path

import pandas as pd
import requests

from models.br_tse_eleicoes.code.python.config import INPUT_DIR, RAW_DATA

API = "https://resultados.tse.jus.br/oficial"
HEADERS = {"User-Agent": "Mozilla/5.0"}
CACHE = RAW_DATA / "api_cache"
MAX_WORKERS = 8  # TSE asks consumers not to hammer the CDN

DETALHE_COLS = [
    "DT_GERACAO", "HH_GERACAO", "ANO_ELEICAO", "CD_TIPO_ELEICAO",
    "NM_TIPO_ELEICAO", "NR_TURNO", "CD_ELEICAO", "DS_ELEICAO", "DT_ELEICAO",
    "TP_ABRANGENCIA", "SG_UF", "SG_UE", "NM_UE", "CD_MUNICIPIO",
    "NM_MUNICIPIO", "NR_ZONA", "CD_CARGO", "DS_CARGO", "QT_APTOS",
    "QT_SECOES_PRINCIPAIS", "QT_SECOES_AGREGADAS", "QT_SECOES_NAO_INSTALADAS",
    "QT_TOTAL_SECOES", "QT_COMPARECIMENTO",
    "QT_ELEITORES_SECOES_NAO_INSTALADAS", "QT_ABSTENCOES",
    "ST_VOTO_EM_TRANSITO", "QT_VOTOS", "QT_VOTOS_CONCORRENTES",
    "QT_TOTAL_VOTOS_VALIDOS", "QT_VOTOS_NOMINAIS_VALIDOS",
    "QT_TOTAL_VOTOS_LEG_VALIDOS", "QT_VOTOS_LEG_VALIDOS",
    "QT_VOTOS_NOM_CONVR_LEG_VALIDOS", "QT_TOTAL_VOTOS_ANULADOS",
    "QT_VOTOS_NOMINAIS_ANULADOS", "QT_VOTOS_LEGENDA_ANULADOS",
    "QT_TOTAL_VOTOS_ANUL_SUBJUD", "QT_VOTOS_NOMINAIS_ANUL_SUBJUD",
    "QT_VOTOS_LEGENDA_ANUL_SUBJUD", "QT_VOTOS_BRANCOS",
    "QT_TOTAL_VOTOS_NULOS", "QT_VOTOS_NULOS", "QT_VOTOS_NULOS_TECNICOS",
    "QT_VOTOS_ANULADOS_APU_SEP", "HH_ULTIMA_TOTALIZACAO",
    "DT_ULTIMA_TOTALIZACAO",
]  # fmt: skip

PARTIDO_COLS = [
    "DT_GERACAO", "HH_GERACAO", "ANO_ELEICAO", "CD_TIPO_ELEICAO",
    "NM_TIPO_ELEICAO", "NR_TURNO", "CD_ELEICAO", "DS_ELEICAO", "DT_ELEICAO",
    "TP_ABRANGENCIA", "SG_UF", "SG_UE", "NM_UE", "CD_MUNICIPIO",
    "NM_MUNICIPIO", "NR_ZONA", "CD_CARGO", "DS_CARGO", "TP_AGREMIACAO",
    "NR_PARTIDO", "SG_PARTIDO", "NM_PARTIDO", "NR_FEDERACAO", "NM_FEDERACAO",
    "SG_FEDERACAO", "DS_COMPOSICAO_FEDERACAO", "SQ_COLIGACAO", "NM_COLIGACAO",
    "DS_COMPOSICAO_COLIGACAO", "ST_VOTO_EM_TRANSITO",
    "QT_VOTOS_LEGENDA_VALIDOS", "QT_VOTOS_NOM_CONVR_LEG_VALIDOS",
    "QT_TOTAL_VOTOS_LEG_VALIDOS", "QT_VOTOS_NOMINAIS_VALIDOS",
    "QT_VOTOS_LEGENDA_ANUL_SUBJUD", "QT_VOTOS_NOMINAIS_ANUL_SUBJUD",
    "QT_VOTOS_LEGENDA_ANULADOS", "QT_VOTOS_NOMINAIS_ANULADOS",
]  # fmt: skip

TP_AGREMIACAO = {"i": "Partido isolado", "f": "Federação", "c": "Coligação"}
# CD_TIPO_ELEICAO / TP_ABRANGENCIA by API election type (ele-c.json "tp")
ABRANGENCIA = {"8": "F", "1": "E", "3": "M"}


def _get_json(url: str, cache_path: Path | None = None) -> dict | None:
    """GET a JSON with retries; ``None`` on 404. Cached gzip on disk if asked."""
    if cache_path is not None and cache_path.exists():
        with gzip.open(cache_path, "rt", encoding="utf-8") as fh:
            return json.load(fh)
    data: dict = {}
    for attempt in range(5):
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            if r.status_code in (403, 404):
                return None
            r.raise_for_status()
            data = r.json()
            break
        except (requests.RequestException, ValueError):
            if attempt == 4:
                raise
            time.sleep(2**attempt)
    if cache_path is not None:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(cache_path, "wt", encoding="utf-8") as fh:
            json.dump(data, fh)
    return data


def _require_json(url: str) -> dict:
    data = _get_json(url)
    if data is None:
        msg = f"{url} returned 403/404"
        raise FileNotFoundError(msg)
    return data


def _election_config(eleicao: int) -> dict:
    cfg = _require_json(f"{API}/comum/config/ele-c.json")
    for pleito in cfg["pl"]:
        for ele in pleito["e"]:
            if int(ele["cd"]) == eleicao:
                return {**ele, "dt_pleito": pleito["dt"]}
    msg = f"eleição {eleicao} not in ele-c.json"
    raise KeyError(msg)


def _zone_jobs(ciclo: str, eleicao: int, ele: dict) -> list[tuple]:
    """(uf, mun, nm_mun, zona, cargo, ds_cargo) for every zone file to fetch."""
    e6 = f"{eleicao:06d}"
    mun_cfg = _require_json(
        f"{API}/{ciclo}/{eleicao}/config/mun-e{e6}-cm.json"
    )
    cargos = [(int(c["cd"]), c["ds"]) for c in ele["abr"][0]["cp"]]
    jobs = []
    for abr in mun_cfg["abr"]:
        uf = abr["cd"]
        for cargo, ds in cargos:
            # Deputado Estadual (7) everywhere but DF; Distrital (8) only DF
            if (cargo == 7 and uf == "df") or (cargo == 8 and uf != "df"):
                continue
            for mu in abr["mu"]:
                for zona in mu["z"]:
                    jobs.append((uf, mu["cd"], mu["nm"], zona, cargo, ds))
    return jobs


def _fetch_zone(ciclo: str, eleicao: int, job: tuple) -> tuple:
    uf, mun, _, zona, cargo, _ = job
    name = f"{uf}{mun}-z{zona}-c{cargo:04d}-e{eleicao:06d}-u.json"
    url = f"{API}/{ciclo}/{eleicao}/dados/{uf}/{name}"
    data = _get_json(url, CACHE / str(eleicao) / uf / f"{name}.gz")
    return job, data


def _secoes_agregadas(ano: int) -> dict[tuple, int]:
    """Aggregated sections per (SG_UF, CD_MUNICIPIO, NR_ZONA), 1st round."""
    base = (
        INPUT_DIR
        / "perfil_eleitorado_local_votacao"
        / f"eleitorado_local_votacao_{ano}"
    )
    files = sorted(base.glob(f"eleitorado_local_votacao_{ano}*.csv"))
    if not files:
        print(f"  WARNING: no eleitorado_local_votacao_{ano}; agregadas = -1")
        return {}
    usecols = ["NR_TURNO", "SG_UF", "CD_MUNICIPIO", "NR_ZONA",
               "DS_TIPO_SECAO_AGREGADA"]  # fmt: skip
    df = pd.concat(
        pd.read_csv(f, sep=";", encoding="latin-1", dtype=str, usecols=usecols)
        for f in files
    )
    df = df[
        (df["NR_TURNO"] == "1") & (df["DS_TIPO_SECAO_AGREGADA"] == "Agregada")
    ]
    counts = df.groupby(["SG_UF", "CD_MUNICIPIO", "NR_ZONA"]).size()
    return {
        (uf, int(mun), int(z)): int(n) for (uf, mun, z), n in counts.items()
    }


def _ds_eleicao(ano: int, eleicao: int, fallback: str) -> str:
    """DS_ELEICAO as the CDN spells it, read from detalhe_votacao_secao."""
    base = INPUT_DIR / "detalhe_votacao_secao" / f"detalhe_votacao_secao_{ano}"
    for f in sorted(base.glob("*.csv")):
        df = pd.read_csv(
            f, sep=";", encoding="latin-1", dtype=str, nrows=5000,
            usecols=["CD_ELEICAO", "DS_ELEICAO"],
        )  # fmt: skip
        hit = df.loc[df["CD_ELEICAO"] == str(eleicao), "DS_ELEICAO"]
        if len(hit):
            return hit.iloc[0]
    return fallback


def _rows(job: tuple, d: dict, meta: dict, agregadas: dict) -> tuple:
    uf, mun, nm_mun, zona, cargo, ds_cargo = job
    sg_uf = uf.upper()
    tp = meta["tp"]
    base = {
        "DT_GERACAO": d["dg"],
        "HH_GERACAO": d["hg"],
        "ANO_ELEICAO": meta["ano"],
        "CD_TIPO_ELEICAO": 2,
        "NM_TIPO_ELEICAO": "Eleição Ordinária",
        "NR_TURNO": d["t"],
        "CD_ELEICAO": d["ele"],
        "DS_ELEICAO": meta["ds_eleicao"],
        "DT_ELEICAO": d.get("dt") or meta["dt_pleito"],
        "TP_ABRANGENCIA": ABRANGENCIA.get(tp, ""),
        "SG_UF": sg_uf,
        "SG_UE": "BR" if tp == "8" else (sg_uf if tp == "1" else mun),
        "NM_UE": meta["nm_ue"].get(sg_uf, sg_uf) if tp != "3" else nm_mun,
        "CD_MUNICIPIO": mun,
        "NM_MUNICIPIO": nm_mun,
        "NR_ZONA": int(zona),
        "CD_CARGO": cargo,
        "DS_CARGO": ds_cargo,
    }
    s, e, v = d["s"], d["e"], d["v"]
    agreg = agregadas.get((sg_uf, int(mun), int(zona)), 0 if agregadas else -1)
    principais = int(s["ts"])
    vl = v.get("vl", "0")
    detalhe = {
        **base,
        "QT_APTOS": e["te"],
        "QT_SECOES_PRINCIPAIS": principais,
        "QT_SECOES_AGREGADAS": agreg,
        "QT_SECOES_NAO_INSTALADAS": s["sni"],
        "QT_TOTAL_SECOES": principais + agreg if agreg >= 0 else -1,
        "QT_COMPARECIMENTO": e["c"],
        "QT_ELEITORES_SECOES_NAO_INSTALADAS": e.get("esni", "0"),
        "QT_ABSTENCOES": e["a"],
        "ST_VOTO_EM_TRANSITO": "N",
        "QT_VOTOS": v["tv"],
        "QT_VOTOS_CONCORRENTES": v["vvc"],
        "QT_TOTAL_VOTOS_VALIDOS": v["vv"],
        "QT_VOTOS_NOMINAIS_VALIDOS": v["vnom"],
        "QT_TOTAL_VOTOS_LEG_VALIDOS": vl,
        "QT_VOTOS_LEG_VALIDOS": vl,
        "QT_VOTOS_NOM_CONVR_LEG_VALIDOS": -1,
        "QT_TOTAL_VOTOS_ANULADOS": v["van"],
        "QT_VOTOS_NOMINAIS_ANULADOS": -1,
        "QT_VOTOS_LEGENDA_ANULADOS": -1,
        "QT_TOTAL_VOTOS_ANUL_SUBJUD": v["vansj"],
        "QT_VOTOS_NOMINAIS_ANUL_SUBJUD": -1,
        "QT_VOTOS_LEGENDA_ANUL_SUBJUD": -1,
        "QT_VOTOS_BRANCOS": v["vb"],
        "QT_TOTAL_VOTOS_NULOS": v["tvn"],
        "QT_VOTOS_NULOS": v["vn"],
        "QT_VOTOS_NULOS_TECNICOS": v["vnt"],
        "QT_VOTOS_ANULADOS_APU_SEP": v["vsan"],
        "HH_ULTIMA_TOTALIZACAO": d["ht"],
        "DT_ULTIMA_TOTALIZACAO": d["dt"],
    }

    partidos = []
    for carg in d["carg"]:
        feds = {f["n"]: f for f in carg.get("fed", [])}
        for agr in carg["agr"]:
            isolado = agr["tp"] == "i"
            for par in agr["par"]:
                fed = feds.get(par.get("nfed") or "", {})
                partidos.append(
                    {
                        **base,
                        "TP_AGREMIACAO": TP_AGREMIACAO.get(
                            agr["tp"], agr["tp"]
                        ),
                        "NR_PARTIDO": par["n"],
                        "SG_PARTIDO": par["sg"],
                        "NM_PARTIDO": par["nm"],
                        "NR_FEDERACAO": fed.get("n", -1),
                        "NM_FEDERACAO": fed.get("nm", "#NULO#"),
                        "SG_FEDERACAO": fed.get("sg", "#NULO#"),
                        "DS_COMPOSICAO_FEDERACAO": fed.get("com", "#NULO#"),
                        "SQ_COLIGACAO": agr["n"],
                        "NM_COLIGACAO": "PARTIDO ISOLADO"
                        if isolado
                        else agr["nm"],
                        "DS_COMPOSICAO_COLIGACAO": agr.get("com", ""),
                        "ST_VOTO_EM_TRANSITO": "N",
                        "QT_VOTOS_LEGENDA_VALIDOS": par.get("tvtl", "0"),
                        "QT_VOTOS_NOM_CONVR_LEG_VALIDOS": -1,
                        "QT_TOTAL_VOTOS_LEG_VALIDOS": par.get("tvtl", "0"),
                        "QT_VOTOS_NOMINAIS_VALIDOS": par.get("tvtn", "0"),
                        "QT_VOTOS_LEGENDA_ANUL_SUBJUD": -1,
                        "QT_VOTOS_NOMINAIS_ANUL_SUBJUD": -1,
                        "QT_VOTOS_LEGENDA_ANULADOS": -1,
                        "QT_VOTOS_NOMINAIS_ANULADOS": -1,
                    }
                )
    return detalhe, partidos


def _write(
    rows: list[dict], cols: list[str], family: str, ano: int, part: str
):
    out = INPUT_DIR / family / f"{family}_{ano}" / f"{family}_{ano}_{part}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=cols).to_csv(
        out,
        sep=";",
        index=False,
        encoding="latin-1",
        quoting=1,
        errors="replace",
    )
    return out


def build(ano: int, eleicoes: list[int]) -> None:
    ciclo = f"ele{ano}"
    agregadas = _secoes_agregadas(ano)
    by_part: dict[str, dict[str, list]] = {}
    for eleicao in eleicoes:
        ele = _election_config(eleicao)
        jobs = _zone_jobs(ciclo, eleicao, ele)
        meta = {
            "ano": ano,
            "tp": ele["tp"],
            "dt_pleito": ele["dt_pleito"],
            "ds_eleicao": _ds_eleicao(ano, eleicao, ele["nm"]),
            "nm_ue": {},
        }
        print(f"  eleição {eleicao} ({ele['nm']}): {len(jobs)} zone files")
        missing = 0
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            for i, (job, data) in enumerate(
                pool.map(partial(_fetch_zone, ciclo, eleicao), jobs), 1
            ):
                if data is None:
                    missing += 1
                    continue
                detalhe, partidos = _rows(job, data, meta, agregadas)
                # presidente goes to the _BR file, as on the CDN
                part = "BR" if ele["tp"] == "8" else job[0].upper()
                bucket = by_part.setdefault(part, {"d": [], "p": []})
                bucket["d"].append(detalhe)
                bucket["p"].extend(partidos)
                if i % 2000 == 0:
                    print(f"    {i}/{len(jobs)} ({time.time() - t0:.0f}s)")
        print(f"    done: {len(jobs) - missing} files, {missing} not found")

    for part, b in sorted(by_part.items()):
        _write(b["d"], DETALHE_COLS, "detalhe_votacao_munzona", ano, part)
        _write(b["p"], PARTIDO_COLS, "votacao_partido_munzona", ano, part)
        print(
            f"  wrote {part}: {len(b['d'])} detalhe rows, {len(b['p'])} partido rows"
        )


def validate_against_official(api_dir: Path, official_dir: Path, family: str):
    """Compare API-built CSVs to the official ones on the columns builders read."""
    keys = ["SG_UF", "CD_MUNICIPIO", "NR_ZONA", "CD_CARGO"]
    if family == "votacao_partido_munzona":
        keys.append("NR_PARTIDO")
        vals = ["QT_VOTOS_NOMINAIS_VALIDOS", "QT_TOTAL_VOTOS_LEG_VALIDOS"]
    else:
        vals = [
            "QT_APTOS", "QT_SECOES_PRINCIPAIS", "QT_SECOES_AGREGADAS",
            "QT_COMPARECIMENTO", "QT_ABSTENCOES", "QT_TOTAL_VOTOS_VALIDOS",
            "QT_VOTOS_NOMINAIS_VALIDOS", "QT_TOTAL_VOTOS_LEG_VALIDOS",
            "QT_VOTOS_BRANCOS", "QT_TOTAL_VOTOS_NULOS",
        ]  # fmt: skip

    def load(d: Path) -> pd.DataFrame:
        files = [f for f in d.glob("*.csv") if "BRASIL" not in f.name]
        df = pd.concat(
            pd.read_csv(f, sep=";", encoding="latin-1", dtype=str)
            for f in files
        )
        df = df[df["NR_TURNO"] == "1"]
        for c in keys[1:] + vals:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        return df[keys + vals]

    a, o = load(api_dir), load(official_dir)
    m = o.merge(
        a, on=keys, how="outer", suffixes=("_off", "_api"), indicator=True
    )
    print(
        f"  {family}: official {len(o)}, api {len(a)}, {m['_merge'].value_counts().to_dict()}"
    )
    both = m[m["_merge"] == "both"]
    for c in vals:
        diff = (both[f"{c}_off"] != both[f"{c}_api"]).sum()
        print(f"    {c}: {diff} mismatches of {len(both)}")
    return m


if __name__ == "__main__":
    build(int(sys.argv[1]), [int(x) for x in sys.argv[2:]])
