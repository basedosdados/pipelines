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


# ------------------------------------------------------------------- limpeza
# O staging da BD é todo STRING por convenção da casa (o modelo dbt faz o
# safe_cast de cada coluna), então o parquet sai com todas as colunas STRING.
# A conversão passa pelo arrow, nunca por `astype(str)`, que renderiza NULL
# como a string literal "nan" — que o safe_cast não devolve para NULL.

SCHEMAS: dict[str, list[str]] = {
    "instituicao": [
        "ano",
        "mes",
        "tipo_consolidado",
        "id_instituicao",
        "nome_instituicao",
        "tcb",
        "td",
        "tc",
        "ti",
        "sr",
        "segmento",
        "id_municipio",
        "id_conglomerado_financeiro",
        "id_conglomerado_prudencial",
        "data_alteracao_segmento",
    ],
    "coluna": [
        "ano",
        "mes",
        "id_relatorio",
        "id_coluna",
        "tipo_consolidado",
        "nome_relatorio",
        "nome_grupo",
        "nome_coluna",
        "nome_coluna_ingles",
        "ordem_coluna",
    ],
    "relatorio": ["ano", "mes", "id_instituicao", "id_coluna", "valor"],
}

# posição -> coluna, para o cadastro (ver `info` com ty=0; `lid` é o índice cN)
_CADASTRO = {
    "id_instituicao": "c0",
    "nome_instituicao": "c2",
    "tcb": "c3",
    "td": "c4",
    "tc": "c6",
    "segmento": "c8",
    "ti": "c13",
    "id_conglomerado_financeiro": "c14",
    "id_conglomerado_prudencial": "c15",
    "data_alteracao_segmento": "c20",
    "sr": "c32",
}


def _clean_str(v: Any) -> str | None:
    """Normaliza para string, preservando NULL — nunca devolve "nan"."""
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _write_table(rows: list[dict], table: str, ano: int, dt: int, outdir):
    """Grava `outdir/<table>/ano=<ano>/data_<dt>.parquet`, tudo STRING."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    cols = SCHEMAS[table]
    arrays = [
        pa.array([_clean_str(r.get(c)) for r in rows], type=pa.string())
        for c in cols
    ]
    tbl = pa.Table.from_arrays(arrays, names=cols)
    dest = outdir / table / f"ano={ano}"
    dest.mkdir(parents=True, exist_ok=True)
    pq.write_table(tbl, dest / f"data_{dt}.parquet", compression="snappy")


def _walk_colunas(cols: list[dict], info: dict, grupo: str | None = None):
    """Percorre as colunas de um relatório, descendo um nível em `sc`.

    As colunas de primeiro nível de vários relatórios são apenas **cabeçalhos
    de grupo** (por exemplo `Empréstimo com Consignação em Folha`), e as
    medidas ficam em `sc` (as faixas de vencimento). O relatório 123 tem 18
    colunas de primeiro nível e 56 subcolunas. Ignorar `sc` perde ~88% das
    células, então a varredura precisa ser recursiva.

    Devolve `(coluna, entrada_info, nome_do_grupo)` só para as folhas com
    `ty=1` e `lid` válido — `ty=0` é atributo cadastral e já está em
    `instituicao`.
    """
    for c in cols:
        m = info.get(c["ifd"])
        nome = (m.get("n") or "").replace("\n", " ").strip() if m else None
        if c.get("sc"):
            yield from _walk_colunas(c["sc"], info, nome)
            continue
        if m is None or m.get("ty") != 1 or m.get("lid") in (None, -1):
            continue
        yield c, m, grupo


def clean_period(entry, outdir, exact, squashed) -> dict[str, Any]:
    """Decodifica uma competência e grava o parquet das três tabelas.

    Devolve estatísticas para validação — contagens por tabela, nomes de
    município não resolvidos e células sem rubrica correspondente. Nada é
    descartado em silêncio.
    """
    import pathlib

    outdir = pathlib.Path(outdir)
    dt = entry["dt"]
    ano, mes = divmod(dt, 100)
    kinds = _files_by_kind(entry)

    info = {i["id"]: i for i in fetch_file(kinds["info"][0])}

    # ---- instituicao (um registro por tipo de consolidado)
    inst: list[dict] = []
    unresolved: list[tuple[str, str]] = []
    for path in sorted(kinds.get("cadastro", [])):
        tipo = path.rsplit("_", 1)[-1].removesuffix(".json")
        for r in fetch_file(path):
            uf = (r.get("c10") or "").strip()
            cidade = (r.get("c11") or "").strip()
            id_mun = (
                resolve_id_municipio(uf, cidade, exact, squashed)
                if cidade
                else None
            )
            if cidade and id_mun is None:
                unresolved.append((uf, cidade))
            row = {"ano": ano, "mes": mes, "tipo_consolidado": tipo}
            row.update({k: r.get(src) for k, src in _CADASTRO.items()})
            row["id_municipio"] = id_mun
            inst.append(row)

    # ---- coluna (definições vêm do próprio índice, em `trel`)
    colunas: list[dict] = []
    rotulada: set[int] = set()
    for f in entry["files"]:
        t = f.get("trel")
        if not t:
            continue
        tipos = [str(s["id"]) for s in (t.get("s") or [])]
        for c, m, grupo in _walk_colunas(t.get("c", []), info):
            rotulada.add(m["lid"])
            colunas.append(
                {
                    "ano": ano,
                    "mes": mes,
                    "id_relatorio": str(t["id"]),
                    "id_coluna": str(m["lid"]),
                    "tipo_consolidado": tipos[0] if tipos else None,
                    "nome_relatorio": t.get("n"),
                    "nome_grupo": grupo,
                    "nome_coluna": (m.get("n") or "")
                    .replace("\n", " ")
                    .strip(),
                    "nome_coluna_ingles": (m.get("ni") or "")
                    .replace("\n", " ")
                    .strip(),
                    "ordem_coluna": c.get("o"),
                }
            )

    # ---- relatorio (fato) — é exatamente o mapa de células
    cells = build_cell_map(kinds.get("dados", []))

    # Nem toda rubrica presente nos dados é referenciada por um relatório da
    # competência: em 2026-03 são 258 de 631 (~43% das células), séries antigas
    # que o IF.data ainda publica mas não exibe mais. Elas têm nome em `info`,
    # então entram em `coluna` com `id_relatorio` nulo em vez de ficarem sem
    # rótulo — nenhum valor é descartado.
    por_lid: dict[int, dict] = {}
    for i in info.values():
        if i.get("ty") == 1 and isinstance(i.get("lid"), int):
            por_lid.setdefault(i["lid"], i)

    orfas = {lid for cm in cells.values() for lid in cm} - rotulada
    for lid in sorted(orfas):
        m = por_lid.get(lid)
        colunas.append(
            {
                "ano": ano,
                "mes": mes,
                "id_relatorio": None,
                "id_coluna": str(lid),
                "tipo_consolidado": None,
                "nome_relatorio": None,
                "nome_grupo": None,
                "nome_coluna": (m.get("n") or "").replace("\n", " ").strip()
                if m
                else None,
                "nome_coluna_ingles": (m.get("ni") or "")
                .replace("\n", " ")
                .strip()
                if m
                else None,
                "ordem_coluna": None,
            }
        )

    fato: list[dict] = []
    sem_rotulo = 0
    for cod, cellmap in cells.items():
        for lid, valor in cellmap.items():
            if valor is None:
                continue
            if lid not in rotulada and lid not in por_lid:
                sem_rotulo += 1  # nem relatório nem `info` — só aí é órfã real
            fato.append(
                {
                    "ano": ano,
                    "mes": mes,
                    "id_instituicao": str(cod),
                    "id_coluna": str(lid),
                    "valor": valor,
                }
            )

    for table, rows in (
        ("instituicao", inst),
        ("coluna", colunas),
        ("relatorio", fato),
    ):
        if rows:
            _write_table(rows, table, ano, dt, outdir)

    return {
        "dt": dt,
        "instituicao": len(inst),
        "coluna": len(colunas),
        "relatorio": len(fato),
        "municipios_nao_resolvidos": unresolved,
        "celulas_sem_rubrica": sem_rotulo,
    }


# ------------------------------------------------------------------ dicionário
# Os rótulos das colunas codificadas do cadastro vêm de `filtro<p>.json`: cada
# filtro traz `ii` (o id em `info`, cujo `lid` é o índice `cN` do cadastro) e
# `d[]` com `{v: chave, n: rótulo PT, ni: rótulo EN}`. O `tipo_consolidado` vem
# do bloco `sel` do índice. `segmento` (c8) é código sem tabela de rótulos
# publicada — fica sem dicionário, de propósito.

# índice `cN` do cadastro -> coluna nas nossas tabelas
_LID_PARA_COLUNA = {3: "tcb", 4: "td", 6: "tc", 13: "ti", 32: "sr"}


def build_dicionario(index: list[dict], outdir) -> dict[str, Any]:
    """Monta o `dicionario` varrendo o `filtro` de todas as competências.

    `cobertura_temporal` sai como `AAAA(1)AAAA` — o intervalo de anos em que
    aquele par (chave, valor) foi observado. Rótulos que mudam de texto ao
    longo da série viram linhas distintas, cada uma com sua cobertura.
    """
    import pathlib

    outdir = pathlib.Path(outdir)
    # (id_tabela, nome_coluna, chave, valor) -> [anos]
    visto: dict[tuple[str, str, str, str], list[int]] = {}

    def marcar(tabela: str, coluna: str, chave: Any, valor: Any, ano: int):
        if chave is None or valor is None:
            return
        visto.setdefault(
            (tabela, coluna, str(chave).strip(), str(valor).strip()), []
        ).append(ano)

    for entry in index:
        ano = entry["dt"] // 100
        for tipo, nomes in period_tipos(entry).items():
            for tabela in ("instituicao", "coluna"):
                marcar(tabela, "tipo_consolidado", tipo, nomes["pt"], ano)

        kinds = _files_by_kind(entry)
        if not kinds.get("filtro"):
            continue
        info = {i["id"]: i for i in fetch_file(kinds["info"][0])}
        for filtro in fetch_file(kinds["filtro"][0]):
            m = info.get(filtro.get("ii"))
            coluna = _LID_PARA_COLUNA.get(m.get("lid")) if m else None
            if coluna is None:
                continue
            for v in filtro.get("d") or []:
                marcar("instituicao", coluna, v.get("v"), v.get("n"), ano)

    linhas = []
    for (tabela, coluna, chave, valor), anos in sorted(visto.items()):
        linhas.append(
            {
                "id_tabela": tabela,
                "nome_coluna": coluna,
                "chave": chave,
                "cobertura_temporal": f"{min(anos)}(1){max(anos)}",
                "valor": valor,
            }
        )

    if linhas:
        import pyarrow as pa
        import pyarrow.parquet as pq

        cols = [
            "id_tabela",
            "nome_coluna",
            "chave",
            "cobertura_temporal",
            "valor",
        ]
        tbl = pa.Table.from_arrays(
            [
                pa.array([_clean_str(r[c]) for r in linhas], type=pa.string())
                for c in cols
            ],
            names=cols,
        )
        dest = outdir / "dicionario"
        dest.mkdir(parents=True, exist_ok=True)
        pq.write_table(tbl, dest / "data.parquet", compression="snappy")

    return {"dicionario": len(linhas)}
