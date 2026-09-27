"""Scrape São Paulo state execution out of SIGEO Lei 131.

SP publishes no bulk file. The only route to its execution data is SIGEO
(`fazenda.sp.gov.br/SigeoLei131`), a classic ASP.NET WebForms app with a 214 KB
`__VIEWSTATE`, driven here one (year, órgão) query at a time. Coverage 2010-2026, from
SIAFEM/SP, updated daily.

**The postback order is load-bearing, and not the obvious one.**

    GET form  ->  set year  ->  set PHASE  ->  set órgão  ->  search  ->  export

Selecting the órgão first, which is what the layout invites, silently produces a form with
no Credor, Licitação, Item or Município controls at all: those fields do not exist in the
DOM until an execution phase checkbox is ticked. The resulting query still succeeds and
still exports -- it just quietly lacks the creditor, which is the column worth having.

Two further traps, both of which read as "the site is blocking us" when they are not:
  * A single POST replaying the three hidden fields returns HTTP 200 and the *same form
    page*, with no result rows. It is not a block; the postback chain is just incomplete.
  * `GET FlexConsDespesaExcel.aspx` returns "Tempo excedido Favor refazer a pesquisa". The
    export is a form POST of `btnExcel`; the GET devtools shows is only the render target.

What SP does NOT provide, which shapes where the output lands: there is no empenho document
number, no date below the year, and `ddlLicitacao` is the *modalidade* (8 options), not a
tender id. So the result is a credor x budget-line x year panel and belongs in
`despesa_anual`, not in the transaction-grain `despesa` table.
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from constants import (
    BROWSER_UA,
    INPUT_DIR,
    SP_CREDOR_TODOS,
    SP_CTL,
    SP_FIRST_YEAR,
    SP_LAST_YEAR,
    SP_SIGEO_FORM,
)

SP_INPUT = INPUT_DIR / "sp"

# Dimensions to request at full detail. Probed against the live form for 2023/29000:
#     1 (elemento)                      ->   908 rows
#     2 (+ unidade gestora)             ->   930 rows
#     3 (+ fonte de recursos)           -> 1,031 rows
#     8 (everything on offer)           -> no result grid at all, and the follow-up
#                                          btnExcel POST returns an ASP.NET Runtime Error
# so the set is deliberately the three that the canonical schema needs, not everything.
#
# `ddlLicitacao` is excluded on purpose: at detail it adds *no column* (k=4 returned a
# header and row count identical to k=3). The procurement modality is a filter in SIGEO,
# never a groupable dimension. Recovering it would mean running the whole cross-product
# once per modality -- 8x the queries for one low-cardinality column -- which is not worth
# it, so `despesa_anual.modalidade_licitacao` is simply null for SP.
DETAIL_DIMENSIONS = (
    "ddlElemento",
    "ddlUge",
    "ddlFonteRecursos",
)

# Empenhado, Liquidado, Pago. See the comment in Sigeo.query for why Dotação Inicial (0)
# and Dotação Atual (1) are excluded rather than simply unused.
SP_EXECUTION_FASES = (2, 3, 4)

HIDDEN = ("__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION")


def _hidden(html: str) -> dict[str, str]:
    out = {}
    for name in HIDDEN:
        m = re.search(rf'id="{name}"[^>]*value="([^"]*)"', html)
        out[name] = m.group(1) if m else ""
    return out


def _options(html: str, ddl: str) -> list[tuple[str, str]]:
    m = re.search(
        rf'id="ctl00_ContentPlaceHolder1_{ddl}"[^>]*>(.*?)</select>',
        html,
        re.S,
    )
    if not m:
        return []
    return re.findall(
        r'<option[^>]*value="([^"]*)"[^>]*>([^<]*)</option>', m.group(1)
    )


def _detail_value(html: str, ddl: str) -> str | None:
    """The '... (Detalhado)' option for a dimension, if the dropdown offers one."""
    for value, label in _options(html, ddl):
        if "Detalhad" in label:
            return value
    return None


class Sigeo:
    def __init__(self) -> None:
        self.s = requests.Session()
        self.s.headers.update(
            {"User-Agent": BROWSER_UA, "Referer": SP_SIGEO_FORM}
        )
        self.html = self.s.get(SP_SIGEO_FORM, timeout=120).text

    def _post(
        self, fields: dict[str, str], target: str = ""
    ) -> requests.Response:
        data = _hidden(self.html)
        data.update(
            {"__EVENTTARGET": target, "__EVENTARGUMENT": "", "__LASTFOCUS": ""}
        )
        data.update(fields)
        return self.s.post(SP_SIGEO_FORM, data=data, timeout=600)

    def years(self) -> list[str]:
        return [v for v, _ in _options(self.html, "ddlAno") if v]

    def orgaos(self) -> list[tuple[str, str]]:
        return [
            (v, t)
            for v, t in _options(self.html, "ddlOrgao")
            if v not in ("", "...")
        ]

    def query(self, year: str, orgao: str) -> bytes:
        """Run one (year, órgão) query and return the exported CSV bytes."""
        base = {SP_CTL + "ddlAno": year}

        # 1. year
        self.html = self._post(base, SP_CTL + "ddlAno").text

        # 2. phase -- MUST precede the órgão selection, and must be execution phases
        # ONLY. Three distinct behaviours, established by probing the live form:
        #   dotação only (0,1)      -> no Credor at all (an appropriation has no creditor)
        #   dotação + execution     -> Credor appears, but Licitação and Item do NOT
        #   execution only (2,3,4)  -> Credor, Licitação, Item and Município all present
        # So Dotação Inicial/Atual are deliberately never requested here; including them
        # costs two columns that matter and buys appropriation totals that are not
        # transactions anyway.
        for idx in SP_EXECUTION_FASES:
            base[f"{SP_CTL}cblFase${idx}"] = "on"
        self.html = self._post(
            base, f"{SP_CTL}cblFase${SP_EXECUTION_FASES[0]}"
        ).text
        # Check for Licitação, not just CGC: the dotação+execution combination passes a
        # CGC-only check while silently dropping the modality and item columns.
        if "ddlLicitacao" not in self.html:
            raise RuntimeError(
                f"{year}/{orgao}: detail controls absent after the phase postback; "
                "SIGEO changed its form flow"
            )

        # 3. órgão (narrows ddlUo / ddlUge to that órgão)
        base[SP_CTL + "ddlOrgao"] = orgao
        self.html = self._post(base, SP_CTL + "ddlOrgao").text

        # 4. filters
        query = dict(base)
        query[SP_CREDOR_TODOS] = "on"
        for ddl in DETAIL_DIMENSIONS:
            value = _detail_value(self.html, ddl)
            if value is not None:
                query[SP_CTL + ddl] = value

        searched = self._post(
            dict(query, **{SP_CTL + "btnPesquisar": "Pesquisar"})
        ).text
        if "gdvDespesas" not in searched:
            raise NoResultGridError(
                f"{year}/{orgao}: search returned no result grid"
            )
        self.html = searched

        # 5. export -- a form POST, never a GET. Returns the full result set rather than
        # the 30-row page the grid shows.
        r = self._post(
            dict(query, **{SP_CTL + "btnExcel": "Exportar em planilha"})
        )
        ctype = r.headers.get("Content-Type", "")
        if "excel" not in ctype.lower():
            raise RuntimeError(
                f"{year}/{orgao}: export returned {ctype!r}, not a sheet"
            )
        return r.content


def _orgao_of(content: bytes) -> str | None:
    """The órgão code the export actually contains, or None if it has no data rows.

    The first field of every data row is "<code> - <name>". An export with only a header
    (the órgão had no spending that exercise) returns None, which is not a mismatch.
    """
    for line in content.decode("latin-1", "replace").splitlines()[1:]:
        if not line.strip():
            continue
        head = line.split(",", 1)[0].strip().strip('"')
        code = head.split(" - ", 1)[0].strip()
        return code or None
    return None


class NoResultGridError(RuntimeError):
    """SIGEO answered normally, with no expenditure for that (exercise, órgão).

    Distinct from every other failure mode on purpose. Órgãos are queried as a full
    year x órgão grid, but a secretariat only answers for the exercises in which it
    existed -- SECRETARIA DE GESTAO E GOVERNO DIGITAL has nothing in 2010. Treating
    that as a fetch failure aborts the whole scrape, which is what happened on the
    first production run: 35 empty pairs out of 544, and the run died having already
    downloaded 509 good files.

    509 + 35 = 544 is the whole grid, and 509 is exactly what the onboarding scrape
    produced, so these are the same legitimately-empty pairs, not lost data.
    """


def main(
    first_year: int = SP_FIRST_YEAR,
    last_year: int = SP_LAST_YEAR,
    pause: float = 1.0,
) -> None:
    SP_INPUT.mkdir(parents=True, exist_ok=True)
    sigeo = Sigeo()
    orgaos = sigeo.orgaos()
    years = [y for y in sigeo.years() if first_year <= int(y) <= last_year]
    print(
        f"{len(years)} years x {len(orgaos)} órgãos = {len(years) * len(orgaos)} queries"
    )

    failures: list[str] = []
    empty: list[str] = []
    for year in years:
        for code, label in orgaos:
            dest = SP_INPUT / f"despesa_{year}_{code}.csv"
            if dest.exists() and dest.stat().st_size > 0:
                continue
            content = None
            # SIGEO can answer with a DIFFERENT órgão than the one requested -- a stale
            # postback surviving into the export. It happened twice in the 2010-2026
            # scrape (2014/41000 returned 39000, 2015/51000 returned 09000), and it is
            # silent: the file is well formed and full of real rows, just the wrong
            # body's. Left unchecked it loses the requested órgão's year entirely. So the
            # export is verified against what was asked for, and a mismatch is retried on
            # a fresh session rather than written.
            for attempt in range(2):
                try:
                    # SIGEO is stateful per session; a fresh one per query keeps a
                    # failure from poisoning every subsequent export with stale
                    # viewstate.
                    candidate = Sigeo().query(year, code)
                except NoResultGridError as exc:
                    # Expected for a body that did not exist in that exercise.
                    print(f"  {year} {code} {label}: {exc}")
                    empty.append(f"{year}/{code}")
                    break
                except (requests.RequestException, RuntimeError) as exc:
                    print(f"  {year} {code} {label}: {exc}")
                    break
                got = _orgao_of(candidate)
                if got is None or got == code:
                    content = candidate
                    break
                print(
                    f"  {year} {code} {label}: export returned órgão {got}, "
                    f"retrying{' once' if attempt == 0 else ''}",
                    flush=True,
                )
                time.sleep(pause)
            if content is None:
                if f"{year}/{code}" not in empty:
                    failures.append(f"{year}/{code}")
                continue
            dest.write_bytes(content)
            print(f"  {year} {code}: {len(content) / 1024:.0f} KB", flush=True)
            time.sleep(pause)

    attempted = len(years) * len(orgaos)
    if empty:
        print(
            f"EMPTY {len(empty)} of {attempted} (exercise, órgão) pairs "
            f"had no expenditure: {sorted(empty)}"
        )
    # An empty is normal; empty EVERYWHERE means SIGEO changed under us and the grid
    # detection now matches nothing. Only the total case is treated as broken: a large
    # empty share is legitimate early in an exercise, when most órgãos have not spent
    # yet, and an incremental run attempts a single year -- refusing on a majority
    # would fail every January for no reason.
    if attempted and len(empty) == attempted:
        print(
            f"REFUSING: all {attempted} pairs came back empty — that is a broken "
            "scrape, not a sparse grid"
        )
        raise SystemExit(1)
    if empty and len(empty) > attempted // 2:
        print(
            f"  WARNING {len(empty)}/{attempted} pairs empty — high, but accepted; "
            "expected early in an exercise"
        )
    if failures:
        print(f"FAILED {len(failures)}: {failures}")
        raise SystemExit(1)
    print(
        f"SP download complete ({attempted - len(empty)} with data, "
        f"{len(empty)} legitimately empty)"
    )


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--first-year", type=int, default=SP_FIRST_YEAR)
    ap.add_argument("--last-year", type=int, default=SP_LAST_YEAR)
    ap.add_argument("--pause", type=float, default=1.0)
    main(**vars(ap.parse_args()))
