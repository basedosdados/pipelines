"""Download the SC, RS and RO state-contract sources.

SC: dados.sc.gov.br "contratos" package, the XLSX rendition (the CSV/JSON carry SC's
    unparseable-free-text defect; the XLSX is structured).
RS: dados.rs.gov.br "contratos-do-estado" package, four typed CSV/zip files.
RO: the CGE-RO public API (`transparencia.api.ro.gov.br/api/v1/contratos`), paginated
    JSON. Unlike SC/RS this one NEEDS a Brazilian IP -- the host geo-fences non-BR.

SC/RS resources are addressed by their CKAN download URLs, which are stable for those
two packages; if a URL 404s, re-resolve it from `package_show?id=<slug>`. Output goes to
input/{sc_contrato,rs_contrato,ro_contrato}/.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import requests

BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)
IN = (
    Path(
        os.environ.get(
            "EXEC_ESTADUAL_DATA_DIR",
            Path.home() / "Downloads" / "br_state_budget_data",
        )
    )
    / "input"
)

SC_BASE = "https://dados.sc.gov.br/dataset/93dab950-e805-4388-8418-cfb3b73f1623/resource"
SC_FILES = {
    "contratos.xlsx": f"{SC_BASE}/28c909f5-7ae2-4d43-bacf-aa40cfe149f7/download/contratos.xlsx",
    "dicionario.xlsx": f"{SC_BASE}/7e5a6853-70c4-4282-b866-2c37856bdefc/download/dicionario-de-dados-contratos-v1.1.xlsx",
}
RS_BASE = "https://dados.rs.gov.br/dataset/a314aad3-3c89-483d-bafe-74fa5679b001/resource"
RS_FILES = {
    "contratos-de-fornecimento-de-bens.zip": f"{RS_BASE}/72738b1a-e093-45b6-89df-b3f6f739a317/download/contratos-de-fornecimento-de-bens.zip",
    "contratos-de-locacoes.zip": f"{RS_BASE}/dd75048f-cac4-4b03-aed5-0d0580aa21ed/download/contratos-de-locacoes.zip",
    "contratos-de-obras.zip": f"{RS_BASE}/83a21d12-7f09-4f4e-90d2-ec7ca62599fc/download/contratos-de-obras.zip",
    "contratos-de-servicos.zip": f"{RS_BASE}/f535dd7c-1d2d-407e-bf88-a768b44c78b6/download/contratos-de-servicos.zip",
}


RO_API = "https://transparencia.api.ro.gov.br/api/v1/contratos"


def fetch(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(
        url, headers={"User-Agent": BROWSER_UA}, timeout=300, stream=True
    ) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
    print(f"  {dest.name}: {dest.stat().st_size:,} bytes")


def fetch_ro_contratos(dest: Path) -> None:
    """Harvest RO's contract registry from the CGE-RO public API.

    One paginated sweep, no date filter, PageSize capped at 100 (~13.3k rows). NEEDS a
    Brazilian IP -- the host geo-fences non-BR callers -- and is slow, so each page is
    retried before giving up.

    Three of the API's own signals are unreliable and MUST NOT be used to stop, all
    measured on 2026-09-21: `ultimaPagina` is `False` even on the real last page;
    `totalElementos`/`totalDePaginas` over-report by ~100 (13,362 / 134 pages claimed,
    13,262 rows / 133 pages actually served); and requesting the page after the last
    (page 134 here) returns **404**, not an empty result. So the sweep stops on the first
    of: a short page (fewer than PageSize rows) or a 404. Asserting against the reported
    total would falsely fail every run.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    rows: list[dict] = []
    page = 1
    while True:
        got: list | None = None
        for attempt in range(4):
            last = attempt == 3
            try:
                r = requests.get(
                    RO_API,
                    params={"Page": page, "PageSize": 100},
                    headers={"User-Agent": BROWSER_UA},
                    timeout=120,
                )
                # A 404 is how the API marks the page past the last row. But a transient
                # 404 (or a 5xx, or a truncated body) must not end the sweep early and
                # store a short mirror, so validate and decode INSIDE the retry: accept a
                # 404 as the end only once the retries are spent, and refuse a missing or
                # non-list `resultados` rather than treating it as an empty final page.
                if r.status_code == 404:
                    if last:
                        got = []
                        break
                    raise RuntimeError(
                        "404 -- retrying in case it is transient"
                    )
                r.raise_for_status()
                payload = r.json().get("resultados")
                if not isinstance(payload, list):
                    raise RuntimeError(
                        f"resultados is {type(payload).__name__}, not a list"
                    )
                got = payload
                break
            except Exception:
                if last:
                    raise
                time.sleep(3)
        # an empty result -- a real empty last page, or a 404 that survived every retry
        if not got:
            break
        rows.extend(got)
        if len(got) < 100:  # a short page is the real last page
            break
        page += 1
    if not rows:
        raise RuntimeError(
            "ro_contrato: harvested 0 rows -- the API or the IP is wrong"
        )
    with open(dest, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False)
    print(f"  ro_contrato: {len(rows):,} rows -> {dest.name}")


def main() -> None:
    print("SC contratos")
    for name, url in SC_FILES.items():
        fetch(url, IN / "sc_contrato" / name)
    print("RS contratos-do-estado")
    for name, url in RS_FILES.items():
        fetch(url, IN / "rs_contrato" / name)
    print("RO contratos (CGE-RO API, needs a Brazilian IP)")
    fetch_ro_contratos(IN / "ro_contrato" / "contratos.json")


if __name__ == "__main__":
    main()
