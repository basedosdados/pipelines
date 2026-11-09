"""
Testes de `pipelines/utils/gcs.py::dump_header`.

O ponto central: `dump_header` sempre detecta o formato real do arquivo em
`data_path` pela extensão — nunca recebe um `source_format` declarado pelo
chamador (ver `ftwca/investigacoes/pipelines/
promocao-dev-prod-pipelines-vs-table-approve-2026-11-03.md`).
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


def test_dump_header_detects_csv(tmp_path):
    _write_csv(tmp_path)

    header_path, source_format = dump_header(data_path=str(tmp_path))

    assert source_format == "csv"
    header_file = next(Path(header_path).rglob("header.csv"))
    assert pd.read_csv(header_file).shape[0] == 1


def test_dump_header_detects_parquet(tmp_path):
    _write_parquet(tmp_path)

    header_path, source_format = dump_header(data_path=str(tmp_path))

    assert source_format == "parquet"
    header_file = next(Path(header_path).rglob("header.parquet"))
    assert pd.read_parquet(header_file).shape[0] == 1


def test_dump_header_raises_when_neither_format_found(tmp_path):
    (tmp_path / "dados.txt").write_text("não é csv nem parquet")

    with pytest.raises(FileNotFoundError):
        dump_header(data_path=str(tmp_path))


def test_dump_header_preserves_hive_partition_folders(tmp_path):
    partition_dir = tmp_path / "ano=2026" / "mes=11"
    partition_dir.mkdir(parents=True)
    _write_csv(partition_dir)

    header_path, source_format = dump_header(data_path=str(tmp_path))

    assert source_format == "csv"
    header_file = next(Path(header_path).rglob("header.csv"))
    assert "ano=2026" in str(header_file)
    assert "mes=11" in str(header_file)
