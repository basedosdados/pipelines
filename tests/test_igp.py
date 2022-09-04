# -*- coding: utf-8 -*-
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from pipelines.datasets.br_fgv_igp.constants import constants as fgv_igp_constants
from pipelines.datasets.br_fgv_igp.flows import ROOT
from pipelines.datasets.br_fgv_igp.tasks import crawler_fgv, clean_fgv_df
from pipelines.datasets.br_fgv_igp.utils import IGPData


def test_igp_di_object_mes(igpdi_mes_mock):
    with patch(
        "pipelines.datasets.br_fgv_igp.utils.IGPData._get_ipea_data",
        return_value=igpdi_mes_mock,
    ):
        igpdi_mensal = IGPData("IGPDI", "mes")
    acum_2021 = igpdi_mensal.df.loc["2021-12-01", "acum_ano"]
    assert round(acum_2021, ndigits=2) == 17.74


def test_igp_di_object_ano(igpdi_ano_mock):
    with patch(
        "pipelines.datasets.br_fgv_igp.utils.IGPData._get_ipea_data",
        return_value=igpdi_ano_mock,
    ):
        igpdi_anual = IGPData("IGPDI", "ano")
    var_2021 = igpdi_anual.df.loc[2021, "var_anual"]
    assert round(var_2021, ndigits=6) == 17.737619


def test_igp_m_object_mes(igpm_mes_mock, igpm_decendios_mock):
    with patch(
        "pipelines.datasets.br_fgv_igp.utils.IGPData._get_ipea_data",
        return_value=igpm_mes_mock,
    ):
        with patch(
            "pipelines.datasets.br_fgv_igp.utils.IGPData._get_decendios",
            return_value=igpm_decendios_mock,
        ):
            igpm_mensal = IGPData("IGPM", "mes")

    df = igpm_mensal.df
    igpm_2021_12 = df.loc[(df.ano == 2021) & (df.mes == 12)].iloc[0]
    assert round(igpm_2021_12.indice, ndigits=3) == 1100.988
    assert round(igpm_2021_12.var_mensal, ndigits=2) == 0.87
    assert round(igpm_2021_12.indice_primeiro_decendio, ndigits=2) == -0.22
    assert round(igpm_2021_12.indice_segundo_decendio, ndigits=2) == 0.43


def test_igp_m_object_ano(igpm_ano_mock):
    with patch(
        "pipelines.datasets.br_fgv_igp.utils.IGPData._get_ipea_data",
        return_value=igpm_ano_mock,
    ):
        igpm_anual = IGPData("IGPM", "ano")

    df = igpm_anual.df
    igpm_2021 = df.loc[df.ano == 2021].iloc[0]
    assert round(igpm_2021.var_anual, ndigits=6) == 17.783212


def test_igp_og_object_mes(igpog_mes_mock):
    with patch(
        "pipelines.datasets.br_fgv_igp.utils.IGPData._get_ipea_data",
        return_value=igpog_mes_mock,
    ):
        igpog_mensal = IGPData("IGPOG", "mes")
    df = igpog_mensal.df
    igpog_2022_06 = df.loc[(df.ano == 2022) & (df.mes == 6)].iloc[0]
    assert round(igpog_2022_06.indice, ndigits=3) == 1158.568


def test_igp_og_object_ano(igpog_ano_mock):
    with patch(
        "pipelines.datasets.br_fgv_igp.utils.IGPData._get_ipea_data",
        return_value=igpog_ano_mock,
    ):
        igpog_anual = IGPData("IGPOG", "ano")
    df = igpog_anual.df
    igpog_2021 = df.loc[df.ano == 2021].iloc[0]
    assert round(igpog_2021.indice_medio, ndigits=5) == 1029.02317


def test_igp_10_object_mes(igp10_mes_mock):
    with patch(
        "pipelines.datasets.br_fgv_igp.utils.IGPData._get_ipea_data",
        return_value=igp10_mes_mock,
    ):
        igp10_mensal = IGPData("IGP10", "mes")
    df = igp10_mensal.df
    igp10_2022_07 = df.loc[(df.ano == 2022) & (df.mes == 7)].iloc[0]
    assert round(igp10_2022_07.indice, ndigits=3) == 1220.204


@pytest.mark.skip("Will be converted to POO")
def test_crawler():
    INDICE = "IGPDI"
    PERIODO = "mes"
    df = crawler_fgv.run(fgv_igp_constants.FGV_INDEX.value.get(INDICE), PERIODO)
    assert isinstance(df, pd.DataFrame)


@pytest.mark.skip("Will be converted to POO")
def test_clean_df_month(igpdi_mensal: pd.DataFrame) -> None:
    """
    Check indice of July 2022
    Args:
        igpdi_mensal (pd.DataFrame): fixture of Monthly IGP-DI

    Returns:
        None

    """
    INDICE = "IGPDI"
    PERIODO = "mes"
    with patch(
        "pipelines.datasets.br_fgv_igp.tasks.crawler_fgv.run",
        return_value=igpdi_mensal,
    ) as mocked:
        df = mocked(INDICE, period=PERIODO)

    df_cleaned = clean_fgv_df.run(df, root=ROOT, period=PERIODO)

    assert df_cleaned == Path("tmp/data/igpdi_mes.csv")


@pytest.mark.skip("Will be converted to POO")
def test_clean_df_year(igpdi_anual: pd.DataFrame) -> None:
    """
    Check indice of July 2022
    Args:
        igpdi_anual (pd.DataFrame): fixture of Monthly IGP-DI

    Returns:
        None

    """
    INDICE = "IGPDI"
    PERIODO = "ano"
    with patch(
        "pipelines.datasets.br_fgv_igp.tasks.crawler_fgv.run",
        return_value=igpdi_anual,
    ) as mocked:
        df = mocked(INDICE, period=PERIODO)

    df_cleaned = clean_fgv_df.run(df, root=ROOT, period=PERIODO)

    assert df_cleaned == Path("tmp/data/igpdi_ano.csv")


@pytest.fixture
def igpdi_mes_mock() -> pd.DataFrame:
    """
    Fixture of Monthly IGP-DI DataFrame

    Returns:
        pd.DataFrame: the fixture of the DataFrame

    """
    df = pd.read_csv("fixtures/igpdi_mes_ipea.csv")
    df.set_index("DATE", drop=True, inplace=True)
    df.index = pd.to_datetime(df.index)
    return df


@pytest.fixture
def igpdi_ano_mock() -> pd.DataFrame:
    """
    Fixture of Annual IGP-DI DataFrame

    Returns:
        pd.DataFrame: the fixture of the DataFrame

    """
    df = pd.read_csv("fixtures/igpdi_mes_ipea.csv")
    df.set_index("DATE", drop=True, inplace=True)
    df.index = pd.to_datetime(df.index)
    return df


@pytest.fixture
def igpm_mes_mock() -> pd.DataFrame:
    """
    Fixture of monthly IGP-M Dataframe
    Returns:
        pd.DataFrame: the fixture of the DataFrame
    """
    df = pd.read_csv("fixtures/igpm_mes_ipea.csv")
    df.set_index("DATE", drop=True, inplace=True)
    df.index = pd.to_datetime(df.index)
    return df


@pytest.fixture
def igpm_mes_1dec_mock() -> pd.DataFrame:
    """
    Fixture of first tenth of the month
    Returns:
        pd.DataFrame: the fixture of the DataFrame
    """
    df = pd.read_csv("fixtures/igpm_mes_ipea_1dec.csv")
    df.set_index("DATE", drop=True, inplace=True)
    df.index = pd.to_datetime(df.index)
    return df


@pytest.fixture
def igpm_mes_2dec_mock() -> pd.DataFrame:
    """
    Fixture of first tenth of the month
    Returns:
        pd.DataFrame: the fixture of the DataFrame
    """
    df = pd.read_csv("fixtures/igpm_mes_ipea_2dec.csv")
    df.set_index("DATE", drop=True, inplace=True)
    df.index = pd.to_datetime(df.index)
    return df


@pytest.fixture
def igpm_decendios_mock() -> pd.DataFrame:
    """
    Fixture for both tenths
    Returns:
        pd.DataFrame
    """
    df = pd.read_csv("fixtures/igpm_mes_decendios.csv")
    df.set_index("DATE", drop=True, inplace=True)
    df.index = pd.to_datetime(df.index)
    return df


@pytest.fixture
def igpm_ano_mock() -> pd.DataFrame:
    """
    Fixture of first tenth of the month
    Returns:
        pd.DataFrame: the fixture of the DataFrame
    """
    df = pd.read_csv("fixtures/igpm_mes_ipea.csv")
    df.set_index("DATE", drop=True, inplace=True)
    df.index = pd.to_datetime(df.index)
    return df


@pytest.fixture
def igpog_mes_mock() -> pd.DataFrame:
    """
    Fixture of IGPOG month
    Returns:
        pd.DataFrame: the fixture of the DataFrame
    """
    df = pd.read_csv("fixtures/igpog_mes_ipea.csv")
    df.set_index("DATE", drop=True, inplace=True)
    df.index = pd.to_datetime(df.index)
    return df


@pytest.fixture
def igpog_ano_mock() -> pd.DataFrame:
    """
    Fixture of IGPOG month
    Returns:
        pd.DataFrame: the fixture of the DataFrame
    """
    df = pd.read_csv("fixtures/igpog_mes_ipea.csv")
    df.set_index("DATE", drop=True, inplace=True)
    df.index = pd.to_datetime(df.index)
    return df


@pytest.fixture
def igp10_mes_mock() -> pd.DataFrame:
    """
    Fixture of IGP10 month
    Returns:
        pd.DataFrame: the fixture of the DataFrame
    """
    df = pd.read_csv("fixtures/igp10_mes_ipea.csv")
    df.set_index("DATE", drop=True, inplace=True)
    df.index = pd.to_datetime(df.index)
    return df
