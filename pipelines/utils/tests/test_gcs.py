"""
Testes de `pipelines/utils/gcs.py::dump_header`.

O ponto central: `dump_header` detecta o formato real do arquivo em
`data_path` pela extensão, em vez de confiar cegamente no `source_format`
declarado — um `source_format` errado não pode mais quebrar com
`FileNotFoundError` (bug real: `build_and_promote` chamado na mão pra
`br_senado_dados_abertos_administrativos.servidor_hora_extra` com o
default `"csv"`, numa tabela em parquet).
"""

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pipelines.utils.gcs import dump_header


def _write_csv(tmp_path, name: str = "dados.csv") -> None:
    pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}).to_csv(
        tmp_path / name, index=False
    )


def _write_parquet(tmp_path, name: str = "dados.parquet") -> None:
    table = pa.table({"a": [1, 2], "b": ["x", "y"]})
    pq.write_table(table, tmp_path / name)


def test_dump_header_csv_matching_declared_format(tmp_path):
    _write_csv(tmp_path)

    header_path, resolved_format = dump_header(
        data_path=str(tmp_path), source_format="csv"
    )

    assert resolved_format == "csv"
    header_file = next(Path(header_path).rglob("header.csv"))
    assert pd.read_csv(header_file).shape[0] == 1


def test_dump_header_parquet_matching_declared_format(tmp_path):
    _write_parquet(tmp_path)

    header_path, resolved_format = dump_header(
        data_path=str(tmp_path), source_format="parquet"
    )

    assert resolved_format == "parquet"
    header_file = next(Path(header_path).rglob("header.parquet"))
    assert pd.read_parquet(header_file).shape[0] == 1


def test_dump_header_resolves_real_format_when_declared_is_wrong(
    tmp_path, caplog
):
    """A tabela é parquet de verdade, mas o chamador passou "csv" (ex. o
    default de `build_and_promote`, sem saber o `source_format` real da
    tabela) — não pode levantar `FileNotFoundError`."""
    _write_parquet(tmp_path)

    header_path, resolved_format = dump_header(
        data_path=str(tmp_path), source_format="csv"
    )

    assert resolved_format == "parquet"
    header_file = next(Path(header_path).rglob("header.parquet"))
    assert pd.read_parquet(header_file).shape[0] == 1


def test_dump_header_resolves_real_format_when_declared_is_wrong_other_way(
    tmp_path,
):
    """Mesmo caso, invertido: declarado parquet, arquivo real é csv."""
    _write_csv(tmp_path)

    header_path, resolved_format = dump_header(
        data_path=str(tmp_path), source_format="parquet"
    )

    assert resolved_format == "csv"
    header_file = next(Path(header_path).rglob("header.csv"))
    assert pd.read_csv(header_file).shape[0] == 1


def test_dump_header_raises_when_neither_format_found(tmp_path):
    (tmp_path / "dados.txt").write_text("não é csv nem parquet")

    with pytest.raises(FileNotFoundError):
        dump_header(data_path=str(tmp_path), source_format="csv")


def test_dump_header_preserves_hive_partition_folders(tmp_path):
    partition_dir = tmp_path / "ano=2026" / "mes=11"
    partition_dir.mkdir(parents=True)
    _write_csv(partition_dir)

    header_path, resolved_format = dump_header(
        data_path=str(tmp_path), source_format="csv"
    )

    assert resolved_format == "csv"
    header_file = next(Path(header_path).rglob("header.csv"))
    assert "ano=2026" in str(header_file)
    assert "mes=11" in str(header_file)
