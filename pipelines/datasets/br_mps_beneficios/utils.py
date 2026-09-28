"""Pure download and cleaning functions for br_mps_beneficios.

No Prefect imports: the one-shot bootstrap under ``models/br_mps_beneficios/code/``
imports these same functions, so the transform exists in one place only.

The source ships the same logical table in three incompatible layouts, and the
reader normalises all of them to one frame before aggregation:

===  ==========================  =====================  ====================
era  package                     layout                 espécie
===  ==========================  =====================  ====================
V1   annual zips 2012-2018       19-col ;-CSV, latin-1  label only
V2   monthly CSV Dec/18-May/23    13-col ;-CSV, latin-1  label only
V3   monthly XLSX Jun/23-        27-col, 2-row header   code + label
===  ==========================  =====================  ====================

``mantidos`` is a fourth layout: fixed-width-padded ;-CSV with no competência
column (it comes from the filename), no espécie code, and labels truncated to
20 characters.
"""

from __future__ import annotations

import csv
import io
import itertools
import json
import re
import unicodedata
import urllib.parse
import urllib.request
import zipfile
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.datasets.br_mps_beneficios.constants import constants

MESES_PT = {
    "janeiro": 1,
    "fevereiro": 2,
    "marco": 3,
    "março": 3,
    "abril": 4,
    "maio": 5,
    "junho": 6,
    "julho": 7,
    "agosto": 8,
    "setembro": 9,
    "outubro": 10,
    "novembro": 11,
    "dezembro": 12,
}


# --------------------------------------------------------------------------
# normalisation helpers
# --------------------------------------------------------------------------
def strip_accents(s: str) -> str:
    return (
        unicodedata.normalize("NFKD", str(s))
        .encode("ascii", "ignore")
        .decode()
    )


def norm_token(s: str) -> str:
    """Lowercase, unaccent, collapse to single-spaced alphanumeric tokens."""
    return re.sub(
        r"\s+", " ", re.sub(r"[^a-z0-9]", " ", strip_accents(s).lower())
    ).strip()


def norm_compact(s: str) -> str:
    """Strip every non-alphanumeric character.

    Needed because the source and the directory disagree on apostrophes and
    hyphens in municipality names: SUIBE writes "Santana do Livramento" where
    IBGE writes "Sant'Ana do Livramento", and the same applies to the
    "d'Água" / "d'Ávila" family. Collapsing to letters and digits makes the
    two forms agree without a per-name exception.
    """
    return re.sub(r"[^a-z0-9]", "", strip_accents(s).lower())


def expand_abbrev(s: str) -> str:
    exp = constants.ABREV_EXPANSION.value
    return " ".join(exp.get(t, t) for t in norm_token(s).split())


# --------------------------------------------------------------------------
# reference tables
# --------------------------------------------------------------------------
def especie_label_index() -> dict[str, int]:
    """Map every spelling of an espécie label to its code.

    Built from the official dictionary under both plain normalisation and
    abbreviation expansion, then the hand-checked alias list.
    """
    idx: dict[str, int] = {}
    for code, label in constants.ESPECIE.value.items():
        idx.setdefault(norm_token(label), code)
        idx.setdefault(expand_abbrev(label), code)
    idx.update(constants.ESPECIE_LABEL_ALIAS.value)
    for key, code in list(constants.ESPECIE_LABEL_ALIAS.value.items()):
        idx.setdefault(expand_abbrev(key), code)
    return idx


def resolve_especie_code(
    label: str, index: dict[str, int] | None = None
) -> int | None:
    """Resolve an espécie label to its code, or None if it stays ambiguous.

    Three passes, in order of decreasing certainty:

    1. exact match after normalisation, and after expanding the known
       abbreviations in ``ABREV_EXPANSION``;
    2. abbreviation-prefix match — SUIBE truncates individual words with a
       full stop ("Aposent. Extranum. Funcionário Público"), so a label matches
       a dictionary entry when it has the same number of words and every word is
       a prefix of the corresponding dictionary word. Only a *unique* match is
       accepted, which is what keeps this from guessing;
    3. otherwise None, and the caller raises rather than writing a null code.

    Pass 2 exists so that a newly abbreviated label does not require a new alias
    entry for every spelling the source invents.
    """
    index = index if index is not None else especie_label_index()
    text = str(label).strip()
    # Some rows carry the code in the label column instead of the label.
    if text.isdigit() and int(text) in constants.ESPECIE.value:
        return int(text)
    for key in (expand_abbrev(label), norm_token(label)):
        if key in index:
            return index[key]

    words = expand_abbrev(label).split()
    if not words:
        return None
    hits = set()
    for code, full in constants.ESPECIE.value.items():
        target = expand_abbrev(full).split()
        if len(target) != len(words):
            continue
        if all(t.startswith(w) for w, t in zip(words, target, strict=True)):
            hits.add(code)
    return next(iter(hits)) if len(hits) == 1 else None


def categoria_index() -> dict[int, str]:
    return {
        c: cat
        for cat, codes in constants.CATEGORIA.value.items()
        for c in codes
    }


def municipio_index() -> dict[tuple[str, str], str]:
    """(sigla_uf, key) -> id_municipio, under both normalisation forms.

    Each name is registered under ``norm_token`` and under ``norm_compact`` so
    that punctuation differences resolve without an exception entry. A compact
    key that would collide with a different municipality in the same UF is
    dropped rather than allowed to resolve ambiguously.
    """
    idx: dict[tuple[str, str], str] = {}
    compact: dict[tuple[str, str], set[str]] = {}

    def add(uf: str, nome: str, id_mun: str) -> None:
        idx[(uf, norm_token(nome))] = id_mun
        compact.setdefault((uf, norm_compact(nome)), set()).add(id_mun)

    with open(constants.MUNICIPIO_DIRECTORY.value, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            add(r["sigla_uf"], r["nome"], r["id_municipio"])
    with open(constants.MUNICIPIO_CROSSWALK.value, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            add(r["sigla_uf"], r["nome_suibe"], r["id_municipio"])

    for key, ids in compact.items():
        if len(ids) == 1:
            idx.setdefault(key, next(iter(ids)))
    return idx


def lookup_municipio(
    idx: dict[tuple[str, str], str], uf: str, nome: str
) -> str | None:
    return idx.get((uf, norm_token(nome))) or idx.get((uf, norm_compact(nome)))


def salario_minimo_for(competencia: int) -> float:
    """Nominal minimum wage in force in a YYYYMM competência."""
    applicable = [
        k for k in constants.SALARIO_MINIMO.value if k <= competencia
    ]
    if not applicable:
        raise ValueError(
            f"no minimum wage on record at or before {competencia}"
        )
    return constants.SALARIO_MINIMO.value[max(applicable)]


def validate_reference_tables() -> None:
    """Fail fast if the espécie/categoria tables drift out of agreement."""
    especies = set(constants.ESPECIE.value)
    mapped = [c for codes in constants.CATEGORIA.value.values() for c in codes]
    missing = especies - set(mapped)
    extra = set(mapped) - especies
    dupes = {c for c in mapped if mapped.count(c) > 1}
    if missing or extra or dupes:
        raise ValueError(
            f"CATEGORIA inconsistent: missing={sorted(missing)} "
            f"extra={sorted(extra)} duplicated={sorted(dupes)}"
        )
    if len(constants.FAIXA_ETARIA_BINS.value) - 1 != len(
        constants.FAIXA_ETARIA_LABELS.value
    ):
        raise ValueError("FAIXA_ETARIA bins and labels disagree")


# --------------------------------------------------------------------------
# field parsers
# --------------------------------------------------------------------------
def parse_competencia(raw) -> int | None:
    """Normalise every competência spelling the source uses to YYYYMM."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    if isinstance(raw, (date,)):
        return raw.year * 100 + raw.month
    s = str(raw).strip()
    if re.fullmatch(r"\d{6}", s):  # V3: 202512
        return int(s)
    m = re.fullmatch(r"(\d{4})-(\d{2})-\d{2}", s)  # V2: 2018-12-01
    if m:
        return int(m.group(1)) * 100 + int(m.group(2))
    m = re.fullmatch(r"([A-Za-zçÇ]+)/(\d{4})", s)  # V1: janeiro/2017
    if m:
        mes = MESES_PT.get(strip_accents(m.group(1)).lower())
        if mes:
            return int(m.group(2)) * 100 + mes
    m = re.fullmatch(r"(\d{4})-(\d{2})", s)
    if m:
        return int(m.group(1)) * 100 + int(m.group(2))
    return None


def parse_data(raw) -> date | None:
    """Parse a birth date; the source writes absent dates as 00/00/0000."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    if isinstance(raw, date):
        return raw
    s = str(raw).strip()
    if not s or s.startswith("00/00"):
        return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d/%m/%y"):
        try:
            return pd.to_datetime(s, format=fmt).date()
        except (ValueError, TypeError):
            continue
    return None


def parse_decimal(raw) -> float | None:
    """Parse Qt SM RMI / Vl MR, which use pt-BR decimal notation in the CSVs."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    s = str(raw).strip()
    if not s:
        return None
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def parse_mun_resid(raw) -> tuple[str | None, str | None]:
    """Split ``Mun Resid`` into (sigla_uf, municipality name).

    The field is documented as "Código da Gerência-Executiva (GEX) seguido da
    sigla da UF e do Município de Residência", so the leading digits are an
    INSS administrative region and carry no municipal identity. The separate
    ``UF`` column is the *granting agency's* UF and disagrees with this one, so
    it must never be used for geography.
    """
    if raw is None:
        return None, None
    s = str(raw).strip()
    if s in constants.MUN_SENTINELS.value or "Zerada" in s:
        return None, None
    m = re.match(r"^\d+\s*-\s*([A-Za-z]{2})\s*-\s*(.+)$", s)
    if not m:
        return None, None
    nome = m.group(2).strip()
    return m.group(1).upper(), (nome or None)


def parse_mun_resid_gex(raw) -> tuple[str | None, str | None, str | None]:
    """Like :func:`parse_mun_resid`, but also returns the GEX prefix.

    The GEX is what makes the truncated município field in benefícios mantidos
    unambiguous, so it is captured here to build that lookup.
    """
    if raw is None:
        return None, None, None
    s = str(raw).strip()
    if s in constants.MUN_SENTINELS.value or "Zerada" in s:
        return None, None, None
    m = re.match(r"^(\d+)\s*-\s*([A-Za-z]{2})\s*-\s*(.+)$", s)
    if not m:
        return None, None, None
    nome = m.group(3).strip()
    return m.group(2).upper(), (nome or None), m.group(1)


def idade_em(nascimento: date | None, competencia: int) -> int | None:
    if nascimento is None:
        return None
    ano, mes = divmod(competencia, 100)
    idade = ano - nascimento.year - ((mes, 1) < (nascimento.month, 1))
    return idade if 0 <= idade <= 130 else None


def faixa_etaria(idade: int | None) -> str | None:
    if idade is None:
        return None
    bins = constants.FAIXA_ETARIA_BINS.value
    labels = constants.FAIXA_ETARIA_LABELS.value
    for i in range(len(labels)):
        if bins[i] <= idade < bins[i + 1]:
            return labels[i]
    return None


def clean_clientela(raw) -> str | None:
    n = norm_token(raw or "")
    if n.startswith("urban"):
        return "urbana"
    if n.startswith("rural"):
        return "rural"
    return None


def clean_sexo(raw) -> str | None:
    n = norm_token(raw or "")
    if n.startswith("masc"):
        return "masculino"
    if n.startswith("fem"):
        return "feminino"
    return None


# --------------------------------------------------------------------------
# readers — one normalised frame out of three source layouts
# --------------------------------------------------------------------------
CONCEDIDO_FIELDS = [
    "competencia",
    "especie_codigo",
    "especie_label",
    "dt_nascimento",
    "sexo",
    "clientela",
    "mun_resid",
    "qt_sm_rmi",
]


def _col(header: list[str], *wanted: str) -> int | None:
    """Locate a column by normalised name, tolerating the source's drift."""
    norm = [norm_token(h or "") for h in header]
    for w in wanted:
        wn = norm_token(w)
        for i, h in enumerate(norm):
            if h == wn:
                return i
    for w in wanted:
        wn = norm_token(w)
        for i, h in enumerate(norm):
            if h.startswith(wn) or (wn.startswith(h) and h):
                return i
    return None


def sniff_encoding(sample: bytes) -> str:
    """Pick the text encoding for a source file from its first bytes.

    The monthly CSVs are not consistently encoded: most are latin-1, but the
    files from roughly May/2020 onwards are UTF-8 with a byte-order mark. Read
    the wrong way round, a UTF-8 header decodes as latin-1 mojibake
    ("CompetÃªncia concessÃ£o") and no column is found at all.
    """
    if sample.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    try:
        sample.decode("utf-8")
    except UnicodeDecodeError:
        return "latin-1"
    # Pure ASCII decodes as either; latin-1 is the safe default for this source.
    return "utf-8" if any(b > 0x7F for b in sample) else "latin-1"


def open_source_text(path: Path):
    """Open a source CSV with its encoding sniffed from the first block."""
    with open(path, "rb") as probe:
        enc = sniff_encoding(probe.read(1 << 16))
    return open(path, encoding=enc, errors="replace", newline="")


def iter_concedido_csv(path: Path, chunk: int = 500_000):
    """Stream a V1/V2 semicolon CSV (latin-1) as normalised record dicts."""
    with open_source_text(path) as f:
        header = next(csv.reader(f, delimiter=";"))
        i_comp = _col(header, "Competência concessão")
        i_esp = _col(header, "Espécie")
        i_nasc = _col(header, "Dt Nascimento", "Data Nascimento")
        i_sexo = _col(header, "Sexo.", "Sexo")
        i_cli = _col(header, "Clientela")
        i_mun = _col(header, "Mun Resid", "Município")
        i_rmi = _col(header, "Qt SM RMI")
        # Only espécie and município are required. The source drops columns
        # without warning: Clientela is absent from May-Dec/2019, and the
        # Feb/2020 file carries the note "mandar continuar, obrigado." in the
        # cell where the competência header belongs. A missing competência
        # falls back to the one in the resource name; anything else is null.
        missing = [
            n
            for n, i in [("espécie", i_esp), ("município", i_mun)]
            if i is None
        ]
        if missing:
            raise ValueError(
                f"{path.name}: columns not found: {missing} in {header}"
            )
        widest = max(
            i
            for i in (i_comp, i_esp, i_nasc, i_sexo, i_cli, i_mun, i_rmi)
            if i is not None
        )

        def cell(row: list[str], i: int | None) -> str | None:
            return row[i] if i is not None and i < len(row) else None

        buf = []
        for row in csv.reader(f, delimiter=";"):
            if len(row) <= widest:
                continue
            buf.append(
                {
                    "competencia": cell(row, i_comp),
                    "especie_codigo": None,
                    "especie_label": cell(row, i_esp),
                    "dt_nascimento": cell(row, i_nasc),
                    "sexo": cell(row, i_sexo),
                    "clientela": cell(row, i_cli),
                    "mun_resid": cell(row, i_mun),
                    "qt_sm_rmi": cell(row, i_rmi),
                }
            )
            if len(buf) >= chunk:
                yield buf
                buf = []
        if buf:
            yield buf


def iter_concedido_xlsx(path: Path, chunk: int = 500_000):
    """Stream a V3 workbook. Row 0 is a title banner, row 1 the real header,
    and ``Espécie``/``CID``/``Despacho`` each occupy a code+label column pair."""
    import openpyxl

    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb[wb.sheetnames[0]]
    rows = ws.iter_rows(values_only=True)
    first = next(rows)
    # Most months open with a one-cell title banner and put the real header on
    # row 2, but some (Jun/2024, which also orders its columns alphabetically)
    # start with the header. When there is no banner, the row already consumed
    # as a candidate header has to be replayed as data, or it is silently lost.
    pending: list[tuple] = []
    if sum(1 for c in first if c) > 3:
        header = list(first)
    else:
        header = list(next(rows))
    i_comp = _col(header, "Competência concessão")
    i_esp = _col(header, "Espécie")
    i_nasc = _col(header, "Dt Nascimento", "Data Nascimento")
    i_sexo = _col(header, "Sexo.", "Sexo")
    i_cli = _col(header, "Clientela")
    i_mun = _col(header, "Mun Resid")
    i_rmi = _col(header, "Qt SM RMI")
    if i_esp is None or i_mun is None:
        raise ValueError(f"{path.name}: header not recognised: {header}")
    # Espécie is a (code, label) pair; _col finds the first of the two.
    i_esp_lab = (
        i_esp + 1
        if i_esp + 1 < len(header)
        and norm_token(header[i_esp + 1] or "") == norm_token("Espécie")
        else i_esp
    )
    buf = []
    for row in itertools.chain(pending, rows):
        if row is None or all(c is None for c in row):
            continue
        buf.append(
            {
                "competencia": row[i_comp] if i_comp is not None else None,
                "especie_codigo": row[i_esp] if i_esp_lab != i_esp else None,
                "especie_label": row[i_esp_lab],
                "dt_nascimento": row[i_nasc] if i_nasc is not None else None,
                "sexo": row[i_sexo] if i_sexo is not None else None,
                "clientela": row[i_cli] if i_cli is not None else None,
                "mun_resid": row[i_mun],
                "qt_sm_rmi": row[i_rmi] if i_rmi is not None else None,
            }
        )
        if len(buf) >= chunk:
            yield buf
            buf = []
    if buf:
        yield buf
    wb.close()


def iter_concedido(path: Path, chunk: int = 500_000):
    if path.suffix.lower() in (".xlsx", ".xls"):
        yield from iter_concedido_xlsx(path, chunk)
    elif path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as z:
            inner = z.namelist()[0]
            tmp = path.parent / inner
            if not tmp.exists():
                z.extract(inner, path.parent)
            try:
                yield from iter_concedido_csv(tmp, chunk)
            finally:
                tmp.unlink(missing_ok=True)
    else:
        yield from iter_concedido_csv(path, chunk)


# --------------------------------------------------------------------------
# aggregation
# --------------------------------------------------------------------------
# Plausible range for Qt SM RMI, the renda mensal inicial expressed in minimum
# wages. The RGPS ceiling is roughly 10; the bound is deliberately loose so that
# only clearly corrupt values are removed.
RMI_SM_MAX = 200.0

GRAIN = [
    "ano",
    "mes",
    "sigla_uf",
    "id_municipio",
    "especie_beneficio",
    "categoria_beneficio",
    "clientela",
    "sexo",
    "faixa_etaria",
]


class UnmappedLabelError(ValueError):
    """Raised when an espécie label has no code, rather than nulling it."""


def aggregate_concedido(
    path: Path,
    competencia_hint: int | None = None,
    gex_sink: dict[tuple[str, str, str], str] | None = None,
) -> tuple[pd.DataFrame, dict]:
    """Aggregate one concedido file to the published grain.

    Returns the aggregated frame and a diagnostics dict recording how many
    rows lost their municipality or their age, which feeds the per-year
    coverage report.

    When ``gex_sink`` is supplied, every resolved (GEX, UF, name) triple is
    recorded into it. That mapping is what later makes the truncated município
    field in benefícios mantidos resolvable.
    """
    esp_idx = especie_label_index()
    cat_idx = categoria_index()
    mun_idx = municipio_index()
    cells: dict[tuple, list[float]] = {}
    diag = {
        "linhas": 0,
        "sem_municipio": 0,
        "municipio_nao_encontrado": 0,
        "sem_idade": 0,
        "sem_competencia": 0,
        "especie_codigo_desconhecido": 0,
        "especie_descartada": 0,
        "rmi_fora_de_faixa": 0,
        "nomes_nao_encontrados": {},
    }
    unmapped: set[str] = set()

    for batch in iter_concedido(path):
        for rec in batch:
            diag["linhas"] += 1
            comp = parse_competencia(rec["competencia"]) or competencia_hint
            if comp is None:
                diag["sem_competencia"] += 1
                continue
            code = rec["especie_codigo"]
            if code is not None:
                # The code cell is not trustworthy on its own: one Jun/2024 row
                # carries a CNPJ, and others carry codes (0, 67) that are not
                # espécies at all. Anything unrecognised falls through to the
                # label, and if that fails too the file is rejected rather than
                # written with an espécie no dictionary can explain.
                try:
                    code = int(str(code).strip())
                except (TypeError, ValueError):
                    code = None
                if code is not None and code not in constants.ESPECIE.value:
                    diag["especie_codigo_desconhecido"] += 1
                    code = None
            if code is None or (isinstance(code, str) and not code.strip()):
                lab = rec["especie_label"]
                code = resolve_especie_code(lab, esp_idx)
                if code is None:
                    if (
                        norm_token(lab)
                        in constants.ESPECIE_LABEL_IGNORAR.value
                    ):
                        diag["especie_descartada"] += 1
                        continue
                    unmapped.add(str(lab))
                    continue
            code = int(code)

            uf, nome, gex = parse_mun_resid_gex(rec["mun_resid"])
            if uf is None:
                diag["sem_municipio"] += 1
                id_mun = None
            else:
                id_mun = lookup_municipio(mun_idx, uf, nome)
                if id_mun is None:
                    diag["municipio_nao_encontrado"] += 1
                    diag["nomes_nao_encontrados"][f"{uf}|{nome}"] = (
                        diag["nomes_nao_encontrados"].get(f"{uf}|{nome}", 0)
                        + 1
                    )
                elif gex_sink is not None and gex:
                    gex_sink[(gex, uf, nome)] = id_mun

            idade = idade_em(parse_data(rec["dt_nascimento"]), comp)
            if idade is None:
                diag["sem_idade"] += 1
            key = (
                comp // 100,
                comp % 100,
                uf,
                id_mun,
                code,
                cat_idx.get(code),
                clean_clientela(rec["clientela"]),
                clean_sexo(rec["sexo"]),
                faixa_etaria(idade),
            )
            rmi = parse_decimal(rec["qt_sm_rmi"])
            # One Jun/2024 row reports a renda mensal inicial of about -2e9
            # minimum wages, which is enough on its own to flip the sign of the
            # whole series total. The INSS ceiling is around 10 minimum wages,
            # and the largest legitimate value observed is 29.7, so anything
            # outside this range is dropped and counted.
            if rmi is not None and not (0.0 <= rmi <= RMI_SM_MAX):
                diag["rmi_fora_de_faixa"] += 1
                rmi = None
            acc = cells.setdefault(key, [0, 0.0])
            acc[0] += 1
            if rmi is not None:
                acc[1] += rmi

    if unmapped:
        raise UnmappedLabelError(
            f"{path.name}: espécie labels with no code: {sorted(unmapped)}. "
            "Add them to constants.ESPECIE_LABEL_ALIAS."
        )

    df = pd.DataFrame(
        [(*k, v[0], v[1]) for k, v in cells.items()],
        columns=[*GRAIN, "quantidade", "valor_total_salarios_minimos"],
    )
    if not df.empty:
        sm = df["ano"] * 100 + df["mes"]
        df["valor_total"] = (
            df["valor_total_salarios_minimos"] * sm.map(salario_minimo_for)
        ).round(2)
        df["valor_total_salarios_minimos"] = df[
            "valor_total_salarios_minimos"
        ].round(3)
        df = df.sort_values(GRAIN, na_position="last").reset_index(drop=True)
    return df, diag


# --------------------------------------------------------------------------
# dicionário de espécies
# --------------------------------------------------------------------------
def build_dicionario_especie() -> pd.DataFrame:
    """The espécie code table, as published, plus the reform-stable grouping."""
    validate_reference_tables()
    cat = categoria_index()
    nat = {
        c: n for n, codes in constants.NATUREZA.value.items() for c in codes
    }
    dupes = [c for n, codes in constants.NATUREZA.value.items() for c in codes]
    if len(dupes) != len(set(dupes)):
        raise ValueError("NATUREZA assigns a code to more than one nature")
    unknown = set(dupes) - set(constants.ESPECIE.value)
    if unknown:
        raise ValueError(
            f"NATUREZA references unknown espécies: {sorted(unknown)}"
        )
    rows = [
        {
            "especie_beneficio": str(code),
            "nome_especie": label,
            "nome_especie_anterior": constants.NOME_ANTERIOR.value.get(code),
            "categoria_beneficio": cat[code],
            "natureza_beneficio": nat.get(code, "previdenciaria"),
            "observacao": constants.OBSERVACAO.value.get(code),
        }
        for code, label in constants.ESPECIE.value.items()
    ]
    return (
        pd.DataFrame(rows)
        .sort_values("especie_beneficio", key=lambda s: s.astype(int))
        .reset_index(drop=True)
    )


# --------------------------------------------------------------------------
# GEX lookup — the key that makes benefícios mantidos geocodable
# --------------------------------------------------------------------------
def truncated_mun_key(gex: str, uf: str, nome: str) -> str:
    """The 20-character field that benefícios mantidos actually stores."""
    return f"{gex}-{uf}-{nome}"[:20]


def build_gex_lookup(
    observed: dict[tuple[str, str, str], str],
) -> pd.DataFrame:
    """Turn (GEX, UF, full name) -> id_municipio into a truncated-key lookup.

    Benefícios mantidos truncates ``Município`` to 20 characters, leaving only
    11 of the name — ambiguous for 6.9% of municipalities on the name alone.
    Including the GEX prefix makes the truncated key unique, so the lookup is
    built from benefícios concedidos, which publishes the GEX, the full name
    and enough rows to cover every municipality.
    """
    keys: dict[str, set[str]] = {}
    meta: dict[str, tuple[str, str, str]] = {}
    for (gex, uf, nome), id_mun in observed.items():
        k = truncated_mun_key(gex, uf, nome)
        keys.setdefault(k, set()).add(id_mun)
        meta[k] = (gex, uf, nome)
    rows = [
        {
            "chave_truncada": k,
            "id_municipio": next(iter(v)),
            "gex": meta[k][0],
            "sigla_uf": meta[k][1],
            "nome": meta[k][2],
        }
        for k, v in sorted(keys.items())
        if len(v) == 1
    ]
    ambiguous = {k: sorted(v) for k, v in keys.items() if len(v) > 1}
    if ambiguous:
        print(
            f"WARNING: {len(ambiguous)} ambiguous truncated keys dropped: "
            f"{list(ambiguous.items())[:5]}"
        )
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------
# parquet output
# --------------------------------------------------------------------------
PA_TYPES = {
    "INT64": pa.int64(),
    "FLOAT64": pa.float64(),
    "STRING": pa.string(),
}


def write_partitioned(
    df: pd.DataFrame,
    outdir: Path,
    table: str,
    partition_cols: list[str] | None = None,
) -> Path:
    """Write hive-partitioned, all-STRING Snappy parquet.

    Every column is cast to string on purpose: staging is all-STRING by house
    convention and the dbt model ``safe_cast``s each column back, so the parquet
    schema carries column order, not types. Emitting typed parquet makes
    BigQuery reject the files against the STRING staging schema that
    ``gcs.dump_header`` infers.

    Two details are load-bearing. Values pass through the architecture's real
    types *first*, so ``ano`` serialises as ``"2018"`` and not ``"2018.0"``, and
    the cast to string goes through arrow rather than ``astype(str)``, which
    would render a NULL as the literal ``"nan"`` and defeat the ``safe_cast``.
    ``id_municipio`` is genuinely NULL wherever the source wrote
    ``00000-Zerada``.

    The partition columns go into the hive path only and NOT into the file
    body. ``basedosdados.Table`` builds the staging schema as
    ``partition_columns + columns``, taking the former from the directory names
    and the latter from the file header, so a partition column present in both
    lands in the external table twice and BigQuery rejects the duplicate. The
    architecture still lists the column, because the materialised table does
    have it — sourced from the path.
    """
    partition_cols = partition_cols or ["ano"]
    tdir = outdir / table
    if df.empty:
        return tdir
    arch = pd.read_csv(constants.ARCHITECTURE_DIR.value / f"{table}.csv")
    order = arch["name"].tolist()
    missing = set(order) - set(df.columns)
    if missing:
        raise ValueError(
            f"{table}: frame is missing architecture columns {sorted(missing)}"
        )
    body = [c for c in order if c not in partition_cols]
    typed = pa.schema(
        [
            pa.field(r["name"], PA_TYPES[r["bigquery_type"]])
            for _, r in arch.iterrows()
            if r["name"] in body
        ]
    )
    as_string = pa.schema([pa.field(c, pa.string()) for c in body])

    out = pd.DataFrame(index=df.index)
    for _, r in arch.iterrows():
        col, bq = r["name"], r["bigquery_type"]
        series = df[col]
        if bq == "INT64":
            series = pd.to_numeric(series, errors="coerce").astype("Int64")
        elif bq == "FLOAT64":
            series = pd.to_numeric(series, errors="coerce").astype("Float64")
        else:
            # Stringify value-by-value so that an integer espécie code becomes
            # "31" while a genuine NULL stays None rather than the string "nan".
            series = series.map(lambda v: None if pd.isna(v) else str(v))
        out[col] = series

    for key, group in out.groupby(partition_cols, sort=True, dropna=False):
        parts = key if isinstance(key, tuple) else (key,)
        pdir = tdir
        for name, value in zip(partition_cols, parts, strict=True):
            pdir = pdir / f"{name}={int(value)}"
        pdir.mkdir(parents=True, exist_ok=True)
        at = pa.Table.from_pandas(
            group[body], schema=typed, preserve_index=False
        )
        pq.write_table(
            at.cast(as_string), pdir / "data.parquet", compression="snappy"
        )
    return tdir


# --------------------------------------------------------------------------
# resource discovery
# --------------------------------------------------------------------------
def _ckan(package: str) -> dict:
    url = constants.CKAN_BASE.value + package
    req = urllib.request.Request(
        url, headers={"User-Agent": "basedosdados/br_mps_beneficios"}
    )
    with urllib.request.urlopen(req, timeout=120) as fh:
        return json.load(fh)["result"]


def _six_digit_competencia(token: str) -> int | None:
    """Read a 6-digit period, which the source writes both ways round.

    ``BEN_CONCEDIDOS_122025.xlsx`` is MMYYYY while
    ``D.SDA.PDA.004.MANATIVOS.202306`` is YYYYMM, so the year has to be
    identified by value rather than by position.
    """
    for m in re.finditer(r"(?<!\d)(\d{6})(?!\d)", token):
        block = m.group(1)
        head, tail = int(block[:4]), int(block[4:])
        if 2000 <= head <= 2099 and 1 <= tail <= 12:
            return head * 100 + tail
        head2, tail2 = int(block[:2]), int(block[2:])
        if 2000 <= tail2 <= 2099 and 1 <= head2 <= 12:
            return tail2 * 100 + head2
    return None


def parse_resource_period(
    name: str, url: str = ""
) -> tuple[int | None, int | None]:
    """Read a competência, or a bare year, from a CKAN resource.

    Returns ``(competencia, ano)`` with exactly one set: the monthly packages
    name a month, the 2012-2018 package names only a year.

    Only the resource name and the URL *basename* are inspected. The annual
    archives live in a directory called "Beneficios concedidos entre dezembro
    de 2012 a novembro de 2018", so searching the whole URL for a month name
    assigns every one of them a spurious November or December competência.
    """
    base = url.rsplit("/", 1)[-1] if url else ""
    for token in (name, base):
        low = strip_accents(token).lower()
        ano_m = re.search(r"\b(20\d{2})\b", low)
        for mes_nome, mes in MESES_PT.items():
            if strip_accents(mes_nome) in low and ano_m:
                return int(ano_m.group(1)) * 100 + mes, None
        m = re.search(r"(?<!\d)(0[1-9]|1[0-2])[-_.](20\d{2})(?!\d)", low)
        if m:
            return int(m.group(2)) * 100 + int(m.group(1)), None
        m = re.search(r"(?<!\d)(20\d{2})[-_.](0[1-9]|1[0-2])(?!\d)", low)
        if m:
            return int(m.group(1)) * 100 + int(m.group(2)), None
        comp = _six_digit_competencia(low)
        if comp:
            return comp, None
    for token in (name, base):
        ano_m = re.search(r"\b(20\d{2})\b", strip_accents(token).lower())
        if ano_m:
            return None, int(ano_m.group(1))
    return None, None


def resolve_concedido_resources() -> list[dict]:
    """Every benefícios concedidos file the portal offers, oldest first.

    Three packages have to be merged because the portal split the series:
    annual archives for 2012-2018, then two monthly packages. Where the same
    competência appears twice the later package wins, though in practice the
    overlap (Dec/2018) is byte-identical in aggregate between the two.
    """
    out: dict[str | int, dict] = {}
    for pkg, era in (
        (constants.PKG_CONCEDIDO_HIST.value, "anual_2012_2018"),
        (constants.PKG_CONCEDIDO_MID.value, "mensal_csv"),
        (constants.PKG_CONCEDIDO_CUR.value, "mensal_xlsx"),
    ):
        for r in _ckan(pkg)["resources"]:
            url = r.get("url") or ""
            if not url:
                continue
            comp, ano = parse_resource_period(r.get("name", ""), url)
            if comp is None and ano is None:
                continue
            key = comp if comp is not None else f"ano-{ano}"
            out[key] = {
                "competencia": comp,
                "ano": ano,
                "url": url,
                "name": r.get("name"),
                "era": era,
                "format": (r.get("format") or "").lower(),
            }
    return sorted(
        out.values(), key=lambda d: d["competencia"] or (d["ano"] or 0) * 100
    )


def resolve_mantido_resources(situacao: str = "ativos") -> list[dict]:
    """Benefícios mantidos files for one situação, one per competência.

    The older package publishes csv/json/xml triplets for the same month, so
    non-CSV renditions are filtered out (note the source misspells the
    extension as ``CVS`` in some object keys).
    """
    want = norm_compact(situacao)
    out: dict[int, dict] = {}
    for pkg in (
        constants.PKG_MANTIDO_MID.value,
        constants.PKG_MANTIDO_CUR.value,
    ):
        for r in _ckan(pkg)["resources"]:
            url = r.get("url") or ""
            name = r.get("name") or ""
            if not url or want not in norm_compact(name + url):
                continue
            if not re.search(r"(csv|cvs)", url, re.I):
                continue
            comp, _ = parse_resource_period(name, url)
            if comp is None:
                continue
            out[comp] = {"competencia": comp, "url": url, "name": name}
    return sorted(out.values(), key=lambda d: d["competencia"])


def download(
    url: str, dest: Path, retries: int = 3, min_bytes: int = 1 << 16
) -> Path:
    """Fetch a resource, reusing a cached copy when one is already present.

    A cached file below ``min_bytes`` is treated as absent and refetched: an S3
    403 or an expired link returns a few hundred bytes of XML that would
    otherwise be cached forever and then fail later as a corrupt archive, far
    from its cause. Every real monthly extract is tens of megabytes.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size >= min_bytes:
        return dest
    if dest.exists():
        dest.unlink()
    safe = urllib.parse.quote(url, safe=":/?&=%+")
    last: Exception | None = None
    for _attempt in range(retries):
        try:
            req = urllib.request.Request(
                safe, headers={"User-Agent": "basedosdados/br_mps_beneficios"}
            )
            tmp = dest.with_suffix(dest.suffix + ".part")
            with (
                urllib.request.urlopen(req, timeout=1800) as fh,
                open(tmp, "wb") as out,
            ):
                while chunk := fh.read(1 << 22):
                    out.write(chunk)
            if tmp.stat().st_size < min_bytes:
                body = tmp.read_bytes()[:200].decode("utf-8", "replace")
                tmp.unlink(missing_ok=True)
                raise RuntimeError(f"response too small for {url}: {body!r}")
            tmp.rename(dest)
            return dest
        except Exception as exc:  # retried, then re-raised as RuntimeError
            last = exc
    raise RuntimeError(
        f"download failed after {retries} attempts: {url}"
    ) from last


# --------------------------------------------------------------------------
# benefícios mantidos
# --------------------------------------------------------------------------
MANTIDO_TRUNC = 20


def truncated_especie_index() -> dict[str, tuple[int | None, str]]:
    """Map a 20-character truncated espécie label to (code or None, categoria).

    Benefícios mantidos publishes neither the espécie code nor the full label —
    the field is fixed-width at 20 characters. Thirteen prefixes are shared by
    more than one espécie ("Aposentadoria por Id" covers 8, 41 and 81), so the
    code is only returned when the prefix identifies a single espécie. The
    categoria is always returned: no published prefix is ambiguous with respect
    to it, which is asserted here rather than assumed.
    """
    cat = categoria_index()
    codes: dict[str, set[int]] = {}
    for code, label in constants.ESPECIE.value.items():
        codes.setdefault(norm_token(label[:MANTIDO_TRUNC]), set()).add(code)
    for label, code in constants.ESPECIE_LABEL_ALIAS.value.items():
        codes.setdefault(norm_token(label[:MANTIDO_TRUNC]), set()).add(code)

    index: dict[str, tuple[int | None, str]] = {}
    conflicts: dict[str, set[str]] = {}
    for key, found in codes.items():
        cats = {cat[c] for c in found}
        if len(cats) > 1:
            conflicts[key] = cats
            continue
        index[key] = (
            (next(iter(found)) if len(found) == 1 else None),
            next(iter(cats)),
        )
    if conflicts:
        raise ValueError(
            "truncated espécie prefixes span more than one categoria, so "
            f"categoria_beneficio would not be recoverable: {conflicts}"
        )
    return index


def gex_lookup_index(path: Path | None = None) -> dict[str, str]:
    """Truncated município key -> id_municipio, from the committed lookup."""
    path = path or constants.MUNICIPIO_GEX_LOOKUP.value
    with open(path, encoding="utf-8") as fh:
        return {
            norm_token(r["chave_truncada"]): r["id_municipio"]
            for r in csv.DictReader(fh)
        }


def iter_mantido(path: Path, chunk: int = 1_000_000):
    """Stream a benefícios mantidos CSV, unzipping on the fly.

    The archive holds a single ~12 GB member, so it is decompressed as a stream
    and never written to disk.
    """
    fields = (
        "Espécie",
        "Clientela",
        "Sexo.",
        "Município",
        "Data Nascimento",
        "Vl MR",
    )

    def emit(handle):
        header = next(csv.reader(handle, delimiter=";"))
        idx = {f: _col(header, f) for f in fields}
        missing = [f for f, i in idx.items() if i is None]
        if missing:
            raise ValueError(
                f"{path.name}: columns not found: {missing} in {header}"
            )
        buf = []
        for row in csv.reader(handle, delimiter=";"):
            if len(row) <= max(i for i in idx.values() if i is not None):
                continue
            buf.append(
                {
                    "especie_label": row[idx["Espécie"]],
                    "clientela": row[idx["Clientela"]],
                    "sexo": row[idx["Sexo."]],
                    "mun_resid": row[idx["Município"]],
                    "dt_nascimento": row[idx["Data Nascimento"]],
                    "vl_mr": row[idx["Vl MR"]],
                }
            )
            if len(buf) >= chunk:
                yield buf
                buf = []
        if buf:
            yield buf

    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as z:
            inner = next(n for n in z.namelist() if n.lower().endswith(".csv"))
            with z.open(inner) as probe:
                enc = sniff_encoding(probe.read(1 << 16))
            with z.open(inner) as raw:
                yield from emit(
                    io.TextIOWrapper(
                        raw, encoding=enc, errors="replace", newline=""
                    )
                )
    else:
        with open_source_text(path) as fh:
            yield from emit(fh)


MANTIDO_GRAIN = [
    "ano",
    "mes",
    "sigla_uf",
    "id_municipio",
    "especie_beneficio",
    "especie_beneficio_rotulo",
    "categoria_beneficio",
    "clientela",
    "sexo",
    "faixa_etaria",
]


def aggregate_mantido(
    path: Path, competencia: int
) -> tuple[pd.DataFrame, dict]:
    """Aggregate one benefícios mantidos file to the published grain.

    The competência is passed in because the file does not contain it — it is
    only in the resource name. Chunks are folded into a running frame rather
    than a Python dict so that peak memory stays bounded on a 12 GB input.
    """
    esp_idx = truncated_especie_index()
    mun_idx = gex_lookup_index()
    ano, mes = divmod(competencia, 100)
    diag = {
        "linhas": 0,
        "sem_municipio": 0,
        "municipio_nao_encontrado": 0,
        "sem_idade": 0,
        "especie_ambigua": 0,
        "rotulos_nao_mapeados": {},
        "chaves_municipio_nao_encontradas": {},
    }
    parts: list[pd.DataFrame] = []

    for batch in iter_mantido(path):
        rows = []
        for rec in batch:
            diag["linhas"] += 1
            rotulo = (rec["especie_label"] or "").strip()
            hit = esp_idx.get(norm_token(rotulo))
            if hit is None:
                diag["rotulos_nao_mapeados"][rotulo] = (
                    diag["rotulos_nao_mapeados"].get(rotulo, 0) + 1
                )
                continue
            code, categoria = hit
            if code is None:
                diag["especie_ambigua"] += 1

            raw_mun = (rec["mun_resid"] or "").strip()
            uf, _nome, _gex = parse_mun_resid_gex(raw_mun)
            if uf is None:
                diag["sem_municipio"] += 1
                id_mun = None
            else:
                id_mun = mun_idx.get(norm_token(raw_mun))
                if id_mun is None:
                    diag["municipio_nao_encontrado"] += 1
                    diag["chaves_municipio_nao_encontradas"][raw_mun] = (
                        diag["chaves_municipio_nao_encontradas"].get(
                            raw_mun, 0
                        )
                        + 1
                    )

            idade = idade_em(parse_data(rec["dt_nascimento"]), competencia)
            if idade is None:
                diag["sem_idade"] += 1
            rows.append(
                (
                    ano,
                    mes,
                    uf,
                    id_mun,
                    str(code) if code is not None else None,
                    rotulo,
                    categoria,
                    clean_clientela(rec["clientela"]),
                    clean_sexo(rec["sexo"]),
                    faixa_etaria(idade),
                    parse_decimal(rec["vl_mr"]) or 0.0,
                )
            )
        if rows:
            frame = pd.DataFrame(rows, columns=[*MANTIDO_GRAIN, "valor_total"])
            frame["quantidade"] = 1
            parts.append(
                frame.groupby(MANTIDO_GRAIN, dropna=False, as_index=False).agg(
                    quantidade=("quantidade", "sum"),
                    valor_total=("valor_total", "sum"),
                )
            )
            if len(parts) >= 8:
                parts = [
                    pd.concat(parts, ignore_index=True)
                    .groupby(MANTIDO_GRAIN, dropna=False, as_index=False)
                    .agg(
                        quantidade=("quantidade", "sum"),
                        valor_total=("valor_total", "sum"),
                    )
                ]

    if not parts:
        return pd.DataFrame(
            columns=[*MANTIDO_GRAIN, "quantidade", "valor_total"]
        ), diag
    out = (
        pd.concat(parts, ignore_index=True)
        .groupby(MANTIDO_GRAIN, dropna=False, as_index=False)
        .agg(
            quantidade=("quantidade", "sum"),
            valor_total=("valor_total", "sum"),
        )
    )
    out["valor_total"] = out["valor_total"].round(2)
    return out.sort_values(MANTIDO_GRAIN, na_position="last").reset_index(
        drop=True
    ), diag
