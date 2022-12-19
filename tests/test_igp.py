# -*- coding: utf-8 -*-
"""
Tests for IGP functins
"""
from unittest.mock import patch

import pandas as pd
import pytest

from pipelines.datasets.br_fgv_igp.utils import IGPData


# pylint: disable=invalid-name, redefined-outer-name


@pytest.mark.skip()
def test_igp_di_object_mes(igpdi_mes_mock):
    """Test for igp_di (monthly)"""

    with patch(
        "pipelines.datasets.br_fgv_igp.utils.IGPData._get_ipea_data",
        return_value=igpdi_mes_mock,
    ):
        igpdi_mensal = IGPData("IGPDI", "mes")
    acum_2021 = igpdi_mensal.df.loc["2021-12-01", "variacao_acumulada_ano"]
    assert round(acum_2021, ndigits=2) == 17.74


@pytest.mark.skip()
def test_igp_di_object_ano(igpdi_ano_mock):
    """Test for igp_di (annual)"""

    with patch(
        "pipelines.datasets.br_fgv_igp.utils.IGPData._get_ipea_data",
        return_value=igpdi_ano_mock,
    ):
        igpdi_anual = IGPData("IGPDI", "ano")
    var_2021 = igpdi_anual.df.loc[2021, "variacao_anual"]
    assert round(var_2021, ndigits=6) == 17.737619


@pytest.mark.skip()
def test_igp_m_object_mes(igpm_mes_mock, igpm_decendios_mock):
    """Test for igp_m (monthly)"""

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
    assert round(igpm_2021_12.variacao_mensal, ndigits=2) == 0.87
    assert round(igpm_2021_12.indice_primeiro_decendio, ndigits=2) == -0.22
    assert round(igpm_2021_12.indice_segundo_decendio, ndigits=2) == 0.43


@pytest.mark.skip()
def test_igp_m_object_ano(igpm_ano_mock):
    """Test for igp_m (annual)"""

    with patch(
        "pipelines.datasets.br_fgv_igp.utils.IGPData._get_ipea_data",
        return_value=igpm_ano_mock,
    ):
        igpm_anual = IGPData("IGPM", "ano")

    df = igpm_anual.df
    igpm_2021 = df.loc[df.ano == 2021].iloc[0]
    assert round(igpm_2021.variacao_anual, ndigits=6) == 17.783212


@pytest.mark.skip()
def test_igp_og_object_mes(igpog_mes_mock):
    """Test for igp_og (monthly)"""

    with patch(
        "pipelines.datasets.br_fgv_igp.utils.IGPData._get_ipea_data",
        return_value=igpog_mes_mock,
    ):
        igpog_mensal = IGPData("IGPOG", "mes")
    df = igpog_mensal.df
    igpog_2022_06 = df.loc[(df.ano == 2022) & (df.mes == 6)].iloc[0]
    assert round(igpog_2022_06.indice, ndigits=3) == 1158.568


@pytest.mark.skip()
def test_igp_og_object_ano(igpog_ano_mock):
    """Test for igp_og (annual)"""

    with patch(
        "pipelines.datasets.br_fgv_igp.utils.IGPData._get_ipea_data",
        return_value=igpog_ano_mock,
    ):
        igpog_anual = IGPData("IGPOG", "ano")
    df = igpog_anual.df
    igpog_2021 = df.loc[df.ano == 2021].iloc[0]
    assert round(igpog_2021.indice_medio, ndigits=5) == 1029.02317


@pytest.mark.skip()
def test_igp_10_object_mes(igp10_mes_mock):
    """Test for igp_10 (monthly)"""

    with patch(
        "pipelines.datasets.br_fgv_igp.utils.IGPData._get_ipea_data",
        return_value=igp10_mes_mock,
    ):
        igp10_mensal = IGPData("IGP10", "mes")
    df = igp10_mensal.df
    igp10_2022_07 = df.loc[(df.ano == 2022) & (df.mes == 7)].iloc[0]
    assert round(igp10_2022_07.indice, ndigits=3) == 1220.204


@pytest.mark.skip()
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


@pytest.mark.skip()
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


@pytest.mark.skip()
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


@pytest.mark.skip()
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


@pytest.mark.skip()
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


@pytest.mark.skip()
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


@pytest.mark.skip()
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


@pytest.mark.skip()
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


@pytest.mark.skip()
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


@pytest.mark.skip()
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
