"""Funções puras de download e limpeza do IF.data (BCB).

Compartilhado entre o onboarding estático (`models/br_bcb_ifdata/code/`) e o
pipeline recorrente trimestral. Sem imports do Prefect.

A API documentada do IF.data (Olinda OData, `servico/IFDATA`) respondia 500 em
todas as chamadas de dados em 2026-08-18, então o download usa a API do próprio
aplicativo IF.data (`www3.bcb.gov.br/ifdata/rest/`). Ela não é documentada, mas é
a que o site usa. O formato é decodificado assim:

    trel<p>_<rel>.json  ->  c[]  ->  ifd  --(info[].id)-->  info entry
    info entry.ty == 0  ->  valor vem do cadastro, coluna `c<lid>`
    info entry.ty == 1  ->  valor vem de dados<p>_<n>.json, célula `lid`
    info entry.lid == -1 ->  coluna não disponível nesta competência/tipo

`dados[].e` é o **código da instituição** (CodInst), não um índice posicional.
"""

from __future__ import annotations

import time
from typing import Any

import requests

BASE = "https://www3.bcb.gov.br/ifdata/rest"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; basedosdados/1.0)"}
INDEXES = ("relatorios2000a2024", "relatorios2025a2030")

# O conjunto de tipos de consolidado **muda ao longo do tempo** — não são
# fixos. Ao longo das 105 competências aparecem seis:
#
#   1004  Conglomerados Prudenciais e Instituições Independentes  201403..202306
#   1005  Conglomerados Financeiros e Instituições Independentes  200003..202603
#   1006  Instituições Individuais                                200003..202603
#   1007  Instituições com Operações de Câmbio                    201412..202306
#   1008  (descontinuado)                                         201206..201403
#   1009  Conglomerados Prudenciais e Instituições Independentes  202309..202603
#
# 1009 substitui 1004 a partir de 2023-09. Por isso os nomes são lidos do
# índice por competência (`sel`), nunca de uma constante.


def _get_json(url: str, *, tries: int = 5, timeout: int = 180) -> Any:
    """GET + parse do JSON, com retry exponencial.

    O www3 do BCB falha de forma **silenciosa**: em vez de um 5xx devolve
    `HTTP 200` com o corpo `Erro interno - Internal error`, então
    `raise_for_status` passa e só o parse quebra. Numa varredura das 105
    competências isso aconteceu em 2 dos 315 arquivos de cadastro, e os dois
    baixaram normalmente na tentativa seguinte — é intermitente, não
    permanente. Por isso o retry precisa olhar o corpo, não só o status.
    """
    last: Exception | None = None
    for attempt in range(tries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            last = exc
            time.sleep(2**attempt)
    raise RuntimeError(f"falha ao baixar {url}") from last


def fetch_index() -> list[dict[str, Any]]:
    """Índice de competências (trimestral). Junta as duas faixas publicadas."""
    periods: list[dict[str, Any]] = []
    for name in INDEXES:
        periods.extend(_get_json(f"{BASE}/{name}"))
    return sorted(periods, key=lambda p: p["dt"])


def list_periods() -> list[int]:
    """Competências disponíveis, como inteiros `AAAAMM` (ex.: 202603)."""
    return [p["dt"] for p in fetch_index()]


def source_max_period() -> int:
    """Competência mais recente publicada na fonte."""
    return max(list_periods())


def fetch_file(path: str) -> Any:
    """Baixa um arquivo JSON do IF.data pelo caminho listado no índice.

    O caminho vem com barra dupla (`ifdata_2025_2030//202603/...`); isso é
    literal e exigido pelo endpoint.
    """
    return _get_json(f"{BASE}/arquivos?nomeArquivo={path}")


def _files_by_kind(period_entry: dict[str, Any]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for f in period_entry["files"]:
        name = f["f"].rsplit("/", 1)[-1]
        kind = name.split(str(period_entry["dt"]))[
            0
        ]  # cadastro/dados/trel/info/...
        out.setdefault(kind, []).append(f["f"])
    return out


def period_tipos(period_entry: dict[str, Any]) -> dict[str, dict[str, str]]:
    """`{id_tipo: {"pt": nome, "en": nome}}` para uma competência.

    Lido do bloco `sel` que o próprio índice já traz — não exige baixar o
    `sel<p>.json`. O conjunto varia por competência (ver a nota no topo).
    """
    out: dict[str, dict[str, str]] = {}
    for f in period_entry["files"]:
        for s in f.get("sel") or []:
            out[str(s["id"])] = {"pt": s["n"], "en": s.get("ni", "")}
    return out


def build_cell_map(paths: list[str]) -> dict[int, dict[int, Any]]:
    """`{cod_inst: {lid: valor}}` a partir dos chunks `dados<p>_<n>.json`."""
    cells: dict[int, dict[int, Any]] = {}
    for p in paths:
        for row in fetch_file(p)["values"]:
            cells.setdefault(row["e"], {}).update(
                {c["i"]: c["v"] for c in row["v"]}
            )
    return cells


# --------------------------------------------------------- crosswalk município
# O IF.data identifica a sede da instituição por UF + **nome** da cidade, sem
# código IBGE, então o `id_municipio` é resolvido por nome. A fonte do
# crosswalk é a API de localidades do IBGE (pública, sem credencial, e a
# autoridade sobre o código de 7 dígitos). O teste dbt `relationships` contra
# `br_bd_diretorios_brasil__municipio` é o portão que verifica o resultado.

IBGE_MUNICIPIOS = (
    "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
)

# Nomes históricos usados pelo IF.data que não existem mais no IBGE (município
# renomeado ou desmembrado). Mapeia para o código atual.
ALIAS_MUNICIPIO: dict[tuple[str, str], str] = {
    ("RO", "NOVA BRASILANDIA"): "1100155",  # Nova Brasilândia D'Oeste
    ("RS", "ELDORADO"): "4306767",  # Eldorado do Sul
    ("TO", "PARAISO DO NORTE DO TOCANTINS"): "1716109",  # Paraíso do Tocantins
    ("RN", "ACU"): "2400208",  # grafia antiga de Assú
}

# O DF tem um único município (Brasília). O IF.data às vezes informa a região
# administrativa na cidade — `BRASILIA SAMAMBAIA`, `BRASILIA TAGUATINGA` —, que
# não é município. Tudo no DF resolve para Brasília.
ID_MUNICIPIO_DF = "5300108"


def normalize_municipio(name: str) -> str:
    """Caixa alta, sem acento e sem pontuação — `Sant'Ana` -> `SANT ANA`."""
    import re
    import unicodedata

    s = unicodedata.normalize("NFKD", name or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^A-Z0-9 ]", " ", s.upper())
    return re.sub(r"\s+", " ", s).strip()


def _squash(name: str) -> str:
    return normalize_municipio(name).replace(" ", "")


def _uf_of(m: dict[str, Any]) -> str | None:
    for path in (
        ("microrregiao", "mesorregiao", "UF"),
        ("regiao-imediata", "regiao-intermediaria", "UF"),
    ):
        node: Any = m
        for key in path:
            node = node.get(key) if isinstance(node, dict) else None
        if node:
            return node["sigla"]
    return None


def build_municipio_crosswalk() -> tuple[dict, dict]:
    """`(exato, sem_espacos)` — ambos `{(sigla_uf, nome): id_municipio}`."""
    exact: dict[tuple[str, str], str] = {}
    squashed: dict[tuple[str, str], str] = {}
    for m in _get_json(IBGE_MUNICIPIOS):
        uf = _uf_of(m)
        if uf is None:
            continue
        code = str(m["id"])
        exact[(uf, normalize_municipio(m["nome"]))] = code
        squashed.setdefault((uf, _squash(m["nome"])), code)
    return exact, squashed


def resolve_id_municipio(
    uf: str, nome: str, exact: dict, squashed: dict
) -> str | None:
    """`id_municipio` (IBGE, 7 dígitos) ou `None` se o nome não resolver.

    Tenta, em ordem: nome normalizado; nome sem espaços (pega `Sant'Ana` vs
    `Santana`); a variante `D OESTE` -> `DO OESTE`; e a tabela de nomes
    históricos. Devolver `None` é deliberado — o chamador deve reportar os
    não-resolvidos em vez de gravar nulo em silêncio.
    """
    uf = (uf or "").strip()
    if uf == "DF":
        return ID_MUNICIPIO_DF
    key = (uf, normalize_municipio(nome))
    if key in exact:
        return exact[key]
    sq = (uf, _squash(nome))
    if sq in squashed:
        return squashed[sq]
    doeste = (uf, _squash(nome).replace("DOESTE", "DOOESTE"))
    if doeste in squashed:
        return squashed[doeste]
    return ALIAS_MUNICIPIO.get(key)
