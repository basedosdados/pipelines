"""Download the SC and RS state-contract sources.

SC: dados.sc.gov.br "contratos" package, the XLSX rendition (the CSV/JSON carry SC's
    unparseable-free-text defect; the XLSX is structured).
RS: dados.rs.gov.br "contratos-do-estado" package, four typed CSV/zip files.

Neither needs a Brazilian IP. Resources are addressed by their CKAN download URLs,
which are stable for these two packages; if a URL 404s, re-resolve it from
`package_show?id=<slug>`. Output goes to input/{sc_contrato,rs_contrato}/.
"""

from __future__ import annotations

import os
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


def main() -> None:
    print("SC contratos")
    for name, url in SC_FILES.items():
        fetch(url, IN / "sc_contrato" / name)
    print("RS contratos-do-estado")
    for name, url in RS_FILES.items():
        fetch(url, IN / "rs_contrato" / name)


if __name__ == "__main__":
    main()
