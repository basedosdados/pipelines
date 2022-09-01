# -*- coding: utf-8 -*-
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from pipelines.datasets.br_fgv_igp.flows import ROOT
from pipelines.datasets.br_fgv_igp.tasks import clean_fgv_df


def test_crawler(igpdi_mensal: pd.DataFrame) -> None:
    """
    Check indice of July 2022
    Args:
        igpdi_mensal (pd.DataFrame): fixture of Monthly IGP-DI

    Returns:
        None

    """
    with patch(
        "pipelines.datasets.br_fgv_igp.tasks.crawler_fgv.run",
        return_value=igpdi_mensal,
    ) as mocked:
        df = mocked("IGP12_IGPDI12", period="mes")

    df_cleaned = clean_fgv_df.run(df, root=ROOT)

    assert df_cleaned == Path("tmp/data/igpdi_mes.csv")


@pytest.fixture
def igpdi_mensal() -> pd.DataFrame:
    """
    Fixture of Monthly IGP-DI DataFrame (from Oct/2021 to Jul/2022)

    Returns:
        pd.DataFrame: the fixture of the DataFrame

    """
    df = pd.DataFrame(
        [
            [2021, 1, 10, "IGP12_IGPDI12", "2021-10-01T00:00:00-03:00", 1081.301],
            [2021, 1, 11, "IGP12_IGPDI12", "2021-11-01T00:00:00-03:00", 1075.022],
            [2021, 1, 12, "IGP12_IGPDI12", "2021-12-01T00:00:00-03:00", 1088.489],
            [2022, 1, 1, "IGP12_IGPDI12", "2022-01-01T00:00:00-03:00", 1110.398],
            [2022, 1, 2, "IGP12_IGPDI12", "2022-02-01T00:00:00-03:00", 1127.077],
            [2022, 1, 3, "IGP12_IGPDI12", "2022-03-01T00:00:00-03:00", 1153.777],
            [2022, 1, 4, "IGP12_IGPDI12", "2022-04-01T00:00:00-03:00", 1158.546],
            [2022, 1, 5, "IGP12_IGPDI12", "2022-05-01T00:00:00-03:00", 1166.542],
            [2022, 1, 6, "IGP12_IGPDI12", "2022-06-01T00:00:00-03:00", 1173.831],
            [2022, 1, 7, "IGP12_IGPDI12", "2022-07-01T00:00:00-03:00", 1169.426],
        ],
        index=[
            "2021-10-01",
            "2021-11-01",
            "2021-12-01",
            "2022-01-01",
            "2022-02-01",
            "2022-03-01",
            "2022-04-01",
            "2022-05-01",
            "2022-06-01",
            "2022-07-01",
        ],
        columns=["YEAR", "DAY", "MONTH", "CODE", "RAW DATE", "indice"],
    )
    df.index.names = ["DATE"]
    return df
