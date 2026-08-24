"""Download e limpeza do br_fnde_fundeb — Indicadores do SIOPE.

Funções puras, sem import do Prefect: o `tasks.py` embrulha o que está aqui, e
quem roda a carga fora do worker chama as mesmas tasks pelo `.fn()`. Uma
implementação só, dois caminhos de execução.

As armadilhas da fonte que estas funções têm que absorver estão no README do
diretório, seção "Estrutura do arquivo":

- linha `Estadual` tem 12 campos, não 14 (COD_MUNI e NOM_MUNI são omitidos);
- `COD_MUNI` tem 6 dígitos e vem como float (`520870.000000000000000000`);
- `COD_GRUP` e `NOM_GRUP_INDI` são constantes e não sobem;
- `VAL_INDI` é percentual em alguns indicadores e reais em outros.
"""

import gzip
from collections import defaultdict
from collections.abc import Iterator
from datetime import date
from itertools import chain
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.br_fnde_fundeb.constants import constants
from pipelines.utils.utils import log


def download_product(product_id: int, dest_dir: Path) -> Path:
    """Baixa o `.txt.gz` de um produto da Plataforma Antonieta de Barros.

    O arquivo é mantido comprimido: o produto 53 tem 45 MB comprimidos e 1,4 GB
    expandidos, e a limpeza lê em stream.

    Args:
        product_id: Id do produto na plataforma — 53 (2021 a 2024) ou 54
            (exercício corrente).
        dest_dir: Diretório onde o arquivo é gravado; criado se não existir.

    Returns:
        O caminho do `.gz` baixado.
    """
    url = constants.ARTIFACT_URL.value.format(
        api=constants.API_BASE.value, product_id=product_id
    )

    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / constants.PRODUCT_FILENAMES.value[product_id]

    try:
        with requests.get(url, stream=True, timeout=60) as response:
            response.raise_for_status()
            with dest_path.open("wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)
    except BaseException:
        dest_path.unlink(missing_ok=True)
        raise

    size_mb = dest_path.stat().st_size / 1024**2
    log(f"Baixado {dest_path.name}: {size_mb:.1f} MB")

    return dest_path


def _iter_rows(path: Path) -> Iterator[dict[str, str]]:
    """Percorre o `.gz` devolvendo uma linha por vez, já resolvida por esfera.

    Descomprime em stream — o arquivo do histórico não cabe confortavelmente em
    memória. O cabeçalho é descartado.

    Args:
        path: Caminho do `.txt.gz` da fonte.

    Yields:
        Um mapa nome do campo da fonte -> valor, para cada linha de dado. As
        linhas estaduais não trazem as chaves ``COD_MUNI`` e ``NOM_MUNI``.

    Raises:
        ValueError: Se uma linha não tiver 12 nem 14 campos — a fonte só produz
            esses dois formatos, e um terceiro significa separador dentro de
            campo (o que mudaria o parsing inteiro) e não pode passar em
            silêncio.
    """

    with gzip.open(path, "rt", encoding=constants.ENCODING.value) as f:
        # pulando o cabeçalho
        next(f)

        # start=2 porque o cabeçalho já saiu: a numeração é a do arquivo.
        for lineno, line in enumerate(f, start=2):
            fields = line.rstrip("\n").split(constants.SEPARATOR.value)

            if len(fields) == constants.FIELDS_MUNICIPALITY.value:
                schema_map = constants.INDEX_MUNICIPALITY.value
            elif len(fields) == constants.FIELDS_STATE.value:
                schema_map = constants.INDEX_STATE.value
            else:
                raise ValueError(
                    f"Linha {lineno} de {path.name} tem {len(fields)} campos, "
                    f"e a fonte só produz {constants.FIELDS_STATE.value} ou "
                    f"{constants.FIELDS_MUNICIPALITY.value}: {line[:200]!r}"
                )

            yield {name: fields[i] for name, i in schema_map.items()}


def _clean_municipality_code(cod_muni: str) -> str:
    """Normaliza o `COD_MUNI` da fonte para o código IBGE de 6 dígitos.

    A fonte manda `520870.000000000000000000` para Goiânia, cujo código IBGE é
    `5208707` — falta o dígito verificador. O 7º dígito NÃO é calculado aqui: os
    6 dígitos são o prefixo do código de 7, e a resolução acontece no modelo dbt
    por join contra `br_bd_diretorios_brasil.municipio` (decisão 4 do README).

    Args:
        cod_muni: Valor cru da coluna ``COD_MUNI``.

    Returns:
        O código de 6 dígitos, como string.
    """

    cleansed_cod_muni = cod_muni.split(".")[0]

    return cleansed_cod_muni


def _indicator_unit(tipo: str, cod_indi: str) -> str | None:
    """Diz se o VAL_INDI daquele indicador é percentual, reais ou nenhum.

    Decide qual das duas colunas de valor a linha preenche. A chave é o par
    esfera/código porque o mesmo ``COD_INDI`` significa indicadores diferentes
    em cada esfera.

    Args:
        tipo: Valor da coluna ``TIPO`` — a esfera.
        cod_indi: Valor da coluna ``COD_INDI``.

    Returns:
        ``constants.UNIT_PERCENT``, ``constants.UNIT_CURRENCY``, ou ``None``
        para os indicadores do grupo 5 (IDEB), que não preenchem nenhuma das
        duas colunas.

    Raises:
        KeyError: Se o par não estiver em ``constants.INDICATOR_UNITS``.
    """

    return constants.INDICATOR_UNITS.value[(tipo, cod_indi)]


def build_indicator_tables(
    source_path: Path,
) -> tuple[list[dict], list[dict], set[tuple[str, str, str, str]]]:
    """Separa o arquivo da fonte nas duas tabelas de fato e no catálogo.

    Uma passada só sobre o arquivo: as linhas vão para a tabela da sua esfera, e
    os pares indicador/nome vistos no caminho alimentam o dicionário.

    Args:
        source_path: Caminho do `.txt.gz` da fonte.

    Returns:
        Uma tripla ``(estadual, municipal, catalogo)``. As duas primeiras são
        listas de linhas nas colunas de ``COLUMNS_STATE`` e
        ``COLUMNS_MUNICIPALITY``; a terceira são os pares
        ``(esfera, cod_indi, nome, ano)`` observados, matéria-prima do
        dicionário.
    """

    state_table = []
    municipality_table = []

    return_set = set()

    for row in _iter_rows(source_path):
        tipo = row["TIPO"]
        unit = _indicator_unit(tipo, row["COD_INDI"])
        value = row["VAL_INDI"]

        table_row = {
            "ano": row["NUM_ANO"],
            "bimestre": row["NUM_PERI"],
            "sigla_uf": row["SIG_UF"],
            "id_indicador": row["COD_INDI"],
            "codigo_indicador": row["COD_EXIB"],
            "valor_percentual": value
            if unit == constants.UNIT_PERCENT.value
            else None,
            "valor_real": value
            if unit == constants.UNIT_CURRENCY.value
            else None,
        }

        if tipo == constants.TIPO_STATE.value:
            state_table.append(table_row)

        elif tipo == constants.TIPO_MUNICIPALITY.value:
            table_row["id_municipio"] = _clean_municipality_code(
                row["COD_MUNI"]
            )

            municipality_table.append(table_row)
        else:
            raise ValueError(f"TIPO desconhecido: {tipo!r}")

        return_set.add(
            (tipo, row["COD_INDI"], row["NOM_INDI"], row["NUM_ANO"])
        )

    return state_table, municipality_table, return_set


def dictionary_rows() -> list[dict]:
    """Devolve a tabela `dicionario`, nas colunas de ``COLUMNS_DICTIONARY``.

    As 112 linhas são fixas e moram em ``constants.DICTIONARY_ROWS``, mantidas
    à mão: a fonte reescreve o nome dos indicadores de tempos em tempos, e
    quando isso acontece a lista é editada. A seção "Os nomes mudam de ano para
    ano" do README do conjunto registra quais mudaram e quando.

    Returns:
        Uma linha por indicador e nome vigente.
    """
    return [
        {
            "id_tabela": id_tabela,
            "nome_coluna": constants.DICTIONARY_COLUMN.value,
            "chave": chave,
            "cobertura_temporal": cobertura,
            "valor": valor,
        }
        for id_tabela, chave, cobertura, valor in (
            constants.DICTIONARY_ROWS.value
        )
    ]


def write_partitioned(
    rows: list[dict],
    columns: list[str],
    output_dir: Path,
) -> Path:
    """Escreve as linhas em parquet particionado por ano, tudo STRING.

    Toda coluna sai STRING de propósito: a staging é all-STRING por convenção da
    casa e o `dump_header` do `gcs.py` estringifica o cabeçalho, então parquet
    tipado é rejeitado na leitura. O modelo dbt faz o `safe_cast` de cada
    coluna.

    Dois detalhes que carregam peso, dos rules do repo:

    - converter para string **pelo arrow**, nunca com `astype(str)` — este
      último escreve NULL como a string literal ``"nan"``, que o `safe_cast` não
      transforma de volta em NULL;
    - passar os tipos reais antes de converter, senão `ano` serializa como
      ``"2021.0"`` e o `safe_cast(ano as int64)` devolve NULL.

    Args:
        rows: Linhas a escrever.
        columns: Ordem normativa das colunas, de ``constants``.
        output_dir: Raiz da tabela; as partições caem em
            ``<output_dir>/ano=<ano>/data.parquet``.

    Returns:
        O caminho de ``output_dir``.
    """

    schema = pa.schema([pa.field(c, pa.string()) for c in columns])
    output_dir.mkdir(parents=True, exist_ok=True)

    if "ano" not in columns:
        table = pa.Table.from_pylist(rows, schema=schema)
        pq.write_table(
            table, output_dir / "data.parquet", compression="snappy"
        )
        log(f"{output_dir.name}: {len(rows)} linhas -> {output_dir}")

        return output_dir

    grouped_by_year = defaultdict(list)

    for row in rows:
        grouped_by_year[row["ano"]].append(row)

    sorted_grouped_by_year = sorted(grouped_by_year.items())

    for year, year_rows in sorted_grouped_by_year:
        partition_dir = output_dir / f"ano={year}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        table = pa.Table.from_pylist(year_rows, schema=schema)

        pq.write_table(
            table, partition_dir / "data.parquet", compression="snappy"
        )

    log(
        f"{output_dir.name}: {len(rows)} linhas em "
        f"{len(grouped_by_year)} partições -> {output_dir}"
    )

    return output_dir


def warn_unknown_names(
    catalog: set[tuple[str, str, str, str]],
) -> list[tuple[str, str, str]]:
    """Avisa quando o arquivo traz um nome de indicador fora do dicionário.

    O `dicionario` é uma lista fixa em ``constants.DICTIONARY_ROWS``, então uma
    reescrita de nome pela fonte a deixa desatualizada sem que nada quebre — o
    dado sobe certo e só o rótulo mente. Esta comparação é o que torna isso
    visível.

    Não levanta erro de propósito: rótulo velho não invalida linha de dado.
    Indicador inteiramente novo já para a execução antes daqui, no
    :func:`_indicator_unit`, cujo mapa é fechado.

    Args:
        catalog: As tuplas ``(esfera, cod_indi, nome, ano)`` do arquivo, vindas
            de :func:`build_indicator_tables`.

    Returns:
        Os trios ``(id_tabela, chave, nome)`` que não estão no dicionário,
        ordenados. Lista vazia quando está tudo registrado.
    """
    table_of = {
        constants.TIPO_STATE.value: constants.TABLE_STATE.value,
        constants.TIPO_MUNICIPALITY.value: constants.TABLE_MUNICIPALITY.value,
    }

    # O ano sai das duas pontas: a pergunta é se o nome está registrado, não em
    # que anos ele valia — a cobertura se resolve ao editar a lista.
    #
    # O `strip` importa: a fonte manda vários nomes com espaço sobrando no fim
    # ("... Nº 33 de30-08-2023 ") e as linhas congeladas estão normalizadas.
    # Sem ele, dois indicadores acusam mudança de nome em toda execução.
    seen = {
        (table_of[esfera], cod_indi, nome.strip())
        for esfera, cod_indi, nome, _ in catalog
    }
    known = {
        (id_tabela, chave, valor)
        for id_tabela, chave, _, valor in constants.DICTIONARY_ROWS.value
    }

    unknown = sorted(seen - known)
    for id_tabela, chave, nome in unknown:
        log(
            f"{id_tabela} {chave}: nome fora de DICTIONARY_ROWS — {nome!r}",
            level="warning",
        )

    return unknown


def clean_all(source_path: Path, output_dir: Path) -> dict:
    """Roda a limpeza completa de um arquivo da fonte.

    Ponto de entrada único da limpeza: é o que a task `clean_siope` chama.

    Args:
        source_path: Caminho do `.txt.gz` baixado.
        output_dir: Raiz onde as tabelas particionadas são escritas.

    Returns:
        Um mapa slug da tabela -> diretório particionado, mais ``"max_date"``
        com o período máximo encontrado no arquivo, que alimenta o poll da
        fonte.
    """

    state, municipality, catalog = build_indicator_tables(source_path)

    dictionary = dictionary_rows()

    state_data_dir = write_partitioned(
        state,
        constants.COLUMNS_STATE.value,
        output_dir / constants.TABLE_STATE.value,
    )

    municipality_data_dir = write_partitioned(
        municipality,
        constants.COLUMNS_MUNICIPALITY.value,
        output_dir / constants.TABLE_MUNICIPALITY.value,
    )

    dictionary_data_dir = write_partitioned(
        dictionary,
        constants.COLUMNS_DICTIONARY.value,
        output_dir / constants.TABLE_DICTIONARY.value,
    )

    warn_unknown_names(catalog)

    year, two_month_period = max(
        (int(row["ano"]), int(row["bimestre"]))
        for row in chain(state, municipality)
    )

    max_date = date(year, 2 * two_month_period, 1).strftime(
        constants.DATE_FORMAT.value
    )

    log(
        f"{source_path.name}: {len(state)} estaduais, "
        f"{len(municipality)} municipais, {len(dictionary)} no dicionário; "
        f"cobertura até {max_date}"
    )

    return {
        constants.TABLE_STATE.value: state_data_dir,
        constants.TABLE_MUNICIPALITY.value: municipality_data_dir,
        constants.TABLE_DICTIONARY.value: dictionary_data_dir,
        "max_date": max_date,
    }
