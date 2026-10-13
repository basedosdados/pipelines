"""Download e limpeza do br_fnde_fundeb — Indicadores do SIOPE.

Funções sem import do Prefect: o `tasks.py` as converte em tasks, e uma carga
executada fora do worker chama essas mesmas tasks pelo `.fn()`. As duas formas
de execução compartilham uma única implementação.

As particularidades da fonte tratadas aqui estão detalhadas na seção "Estrutura
do arquivo" do README do conjunto:

- linha `Estadual` tem 12 campos, não 14 (COD_MUNI e NOM_MUNI são omitidos);
- `COD_MUNI` tem 6 dígitos e vem como float (`520870.000000000000000000`);
- `COD_GRUP` e `NOM_GRUP_INDI` são constantes e não sobem;
- `VAL_INDI` é percentual em alguns indicadores e reais em outros.
"""

import gzip
from collections import defaultdict
from collections.abc import Iterator
from datetime import date, datetime
from itertools import chain
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.br_fnde_fundeb.constants import constants
from pipelines.utils.utils import log


def source_update_date(product_id: int) -> str:
    """Data em que a plataforma gravou o arquivo do produto.

    É o mesmo `Arquivo atualizado em` que a página do produto exibe, servido em
    UTC pelo endpoint de metadados do artefato e convertido para São Paulo.

    Args:
        product_id: Id do produto na plataforma — 53 ou 54.

    Returns:
        A data de gravação no formato de `constants.DATE_FORMAT`.
    """
    url = constants.ARTIFACT_METADATA_URL.value.format(
        api=constants.API_BASE.value, product_id=product_id
    )

    response = requests.get(url, timeout=60)
    response.raise_for_status()
    metadata = response.json()

    stored_at = datetime.fromisoformat(
        metadata["lastUpdated"].replace("Z", "+00:00")
    ).astimezone(ZoneInfo("America/Sao_Paulo"))
    update_date = stored_at.strftime(constants.DATE_FORMAT.value)

    log(
        f"Produto {product_id}: {metadata['name']} gravado em "
        f"{stored_at:%d/%m/%Y %H:%M:%S}, {metadata['size'] / 1024**2:.1f} MB"
    )

    return update_date


def download_product(product_id: int, dest_dir: Path) -> Path:
    """Baixa o `.txt.gz` de um produto da Plataforma Antonieta de Barros.

    O arquivo é mantido comprimido: o produto 53 tem 51 MB comprimidos e mais de
    1 GB expandido, e a limpeza lê em stream.

    Args:
        product_id: Id do produto na plataforma — 53 (exercícios fechados) ou 54
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
    memória. A primeira linha, o cabeçalho, é descartada, e a contagem começa em
    2 para que o número citado no erro seja o do arquivo.

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
        next(f)

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

    A fonte publica `520870.000000000000000000` para Goiânia, cujo código IBGE é
    `5208707`: falta o dígito verificador. O 7º dígito não é calculado aqui — os
    6 dígitos são o prefixo do código completo, e o modelo dbt resolve o restante
    por join contra `br_bd_diretorios_brasil.municipio`.

    Args:
        cod_muni: Valor cru da coluna ``COD_MUNI``.

    Returns:
        O código de 6 dígitos, como string.
    """

    cleansed_cod_muni = cod_muni.split(".")[0]

    return cleansed_cod_muni


def _indicator_unit(tipo: str, cod_indi: str) -> str | None:
    """Informa se o VAL_INDI daquele indicador é percentual, reais ou nenhum.

    Determina qual das duas colunas de valor a linha preenche. A chave é o par
    esfera/código porque o mesmo ``COD_INDI`` designa indicadores diferentes em
    cada esfera.

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

    Percorre o arquivo uma vez: cada linha vai para a tabela da sua esfera, e os
    pares indicador/nome encontrados formam o catálogo.

    Args:
        source_path: Caminho do `.txt.gz` da fonte.

    Returns:
        Uma tripla ``(estadual, municipal, catalogo)``. As duas primeiras são
        listas de linhas nas colunas de ``COLUMNS_STATE`` e
        ``COLUMNS_MUNICIPALITY``; a terceira reúne os pares
        ``(esfera, cod_indi, nome, ano)`` observados, que
        :func:`warn_unknown_names` confere contra o dicionário.
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

    As 112 linhas são fixas, estão em ``constants.DICTIONARY_ROWS`` e são
    mantidas à mão: a fonte reescreve o nome dos indicadores ao longo da série, e
    a lista é editada quando isso ocorre. A seção "Os nomes mudam de ano para
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
    """Escreve as linhas em parquet particionado por ano, com todas as colunas
    em STRING.

    A staging é all-STRING por convenção da casa, e o `dump_header` do `gcs.py`
    converte o cabeçalho para string — parquet tipado é rejeitado na leitura. O
    modelo dbt faz o `safe_cast` de cada coluna para o tipo da arquitetura.

    Os valores chegam aqui como texto cru da fonte e vão para o arrow sem passar
    por número, o que preserva NULL como NULL (`astype(str)` o escreveria como a
    string ``"nan"``) e mantém `ano` como ``"2021"`` em vez de ``"2021.0"``.

    Tabelas sem coluna `ano` — o `dicionario` — saem num arquivo único, sem
    diretório de partição.

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

    O `dicionario` é a lista fixa de ``constants.DICTIONARY_ROWS``, e uma
    reescrita de nome pela fonte a deixa desatualizada sem interromper a carga:
    o dado sobe correto e só o rótulo fica errado. Esta comparação identifica
    quando a lista precisa ser editada.

    Registra WARNING e não levanta erro, porque rótulo desatualizado não
    invalida linha de dado. Indicador inédito interrompe a execução antes daqui,
    em :func:`_indicator_unit`, cujo mapa é fechado.

    A comparação ignora o ano nas duas pontas — o que se verifica é se o nome
    está registrado, não em que anos ele vale — e normaliza o espaço nas bordas,
    porque a fonte publica vários nomes com espaço sobrando no fim
    ("... Nº 33 de30-08-2023 ") enquanto ``DICTIONARY_ROWS`` está normalizada.
    Sem essa normalização, dois indicadores municipais apareceriam como nome
    novo em toda execução.

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
    """Executa a limpeza completa de um arquivo da fonte.

    Ponto de entrada da limpeza, chamado pela task :func:`clean_siope`: monta as
    três tabelas, escreve as partições e confere os nomes de indicador contra o
    dicionário.

    Args:
        source_path: Caminho do `.txt.gz` baixado.
        output_dir: Raiz onde as tabelas particionadas são escritas.

    Returns:
        Um mapa slug da tabela -> diretório particionado, mais ``"max_date"``
        com o último bimestre encontrado no arquivo, registrado no log da
        execução.
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
