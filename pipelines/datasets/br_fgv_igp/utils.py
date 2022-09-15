# -*- coding: utf-8 -*-
"""
General purpose functions for the br_fgv_igp project
"""
# pylint: disable=invalid-name,too-many-instance-attributes
import ipeadatapy as idpy
import numpy as np
import pandas as pd

from pipelines.datasets.br_fgv_igp.constants import constants as fgv_constants

FGV_INDEX = fgv_constants.FGV_INDEX.value
ROOT = fgv_constants.ROOT.value


class IGPData:
    """ Class for IGP pipeline """
    def __init__(self, ipeacode, period):
        self.fgv_indexes = FGV_INDEX.get(ipeacode)
        self.period = period
        self.main_index = self.fgv_indexes[0]
        self.first_dec = self.fgv_indexes[1]
        self.second_dec = self.fgv_indexes[2]
        self.decendios = self.fgv_indexes[1:3]
        self.index_name = ipeacode
        self.df = self._create_dataframe()

    @property
    def filepath(self) -> str:
        """
        The path of the file that will be saved to be uploaded to GCS
        Returns:
            str: full path of csv file
        """
        return f"{ROOT}/{self.index_name.lower()}_{self.period.lower()}/{self.index_name.lower()}_{self.period.lower()}.csv"

    def _get_ipea_data(self) -> pd.DataFrame:
        """
        Method to get the DataFrame from Ipeadata
        Returns:
            pd.DataFrame: DataFrame from Ipeadata with FGV data
        """
        return idpy.timeseries(self.main_index)

    def _create_dataframe(self) -> pd.DataFrame:
        """
        Creates basic DataFrame, according to period
        Returns:
            pd.DataFrame
        """
        df = self._get_ipea_data()
        df.rename({df.columns[-1]: "indice"}, axis=1, inplace=True)
        df["indice"] = df["indice"].astype(float, errors="ignore")
        df.rename(columns={"YEAR": "ano", "MONTH": "mes"}, inplace=True)
        df.drop(columns=["DAY", "CODE", "RAW DATE"], inplace=True)

        if self.period == "mes":
            df = self.month_dataframe(df)
        if self.period == "ano":
            df = self.year_dataframe(df)

        return df

    def month_dataframe(self, dfm: pd.DataFrame) -> pd.DataFrame:
        """
        Complement Ipea data with FGV information
        Args:
            dfm (pd.DataFrame: DataFrame to be modelled

        Returns:
            pd.DataFrame: DataFrame with specific columns
        """
        dfm["var_mensal"] = dfm["indice"].pct_change(periods=1) * 100
        dfm["var_12_meses"] = dfm["indice"].pct_change(periods=12) * 100
        if self.first_dec and self.second_dec:
            decendios = self._get_decendios()
            dfm = pd.merge(dfm, decendios, how="outer", on="DATE")
        # noinspection PyUnresolvedReferences
        dfm["acum_ano"] = (
            dfm[["var_mensal"]]
            .groupby(dfm.index.year)
            .apply(self._calculate_year_accum)
        )
        dfm["next_month"] = dfm["indice"].shift(-1)
        dfm["indice_fechamento_mensal"] = round(
            np.sqrt(dfm["indice"] * dfm["next_month"]), ndigits=6
        )
        dfm.drop(columns=["next_month"], inplace=True)
        return dfm

    def year_dataframe(self, dff: pd.DataFrame) -> pd.DataFrame:
        """
        Complement Ipea data with FGV information
        Args:
            dff (pd.DataFrame: DataFrame to be modelled

        Returns:
            pd.DataFrame: DataFrame with specific columns

        """
        # noinspection PyUnresolvedReferences
        grouped_df = dff.groupby(dff.index.year).mean()
        grouped_df.rename(columns={"indice": "indice_medio"}, inplace=True)
        grouped_df.index.name = "YEAR"
        grouped_df.drop(columns=["mes"], inplace=True)
        last_days = dff.loc[dff["mes"] == 12]
        last_days.set_index("ano", inplace=True)
        last_days.index.name = "YEAR"
        last_days.drop(columns=["mes"], inplace=True)
        dfy = pd.merge(grouped_df, last_days, how="outer", on="YEAR")
        dfy.drop(dfy.tail(1).index, inplace=True)
        dfy["var_anual"] = dfy["indice"].pct_change(periods=1) * 100
        dfy["next_month"] = dfy["indice"].shift(-1)
        dfy["indice_fechamento_anual"] = round(
            np.sqrt(dfy["indice"] * dfy["next_month"]), ndigits=6
        )
        dfy.drop(columns=["next_month"], inplace=True)
        return dfy

    def _get_decendios(self) -> pd.DataFrame:
        """
        Get the 2 tenths of IGP-M, as calculated by FGV
        Returns:
            pd.DataFrame: DataFrame with 1st and 2nd tenths
        """
        dec1 = idpy.timeseries(self.decendios[0])
        dec1.rename({dec1.columns[-1]: "var_primeiro_decendio"}, axis=1, inplace=True)
        dec1 = dec1[["var_primeiro_decendio"]]
        dec2 = idpy.timeseries(self.decendios[1])
        dec2.rename({dec2.columns[-1]: "var_segundo_decendio"}, axis=1, inplace=True)
        dec2 = dec2[["var_segundo_decendio"]]
        return pd.merge(dec1, dec2, how="outer", on="DATE")

    @staticmethod
    def _calculate_year_accum(row: pd.Series) -> pd.Series:
        """
        Calculates accumulated index in the year
        Returns:
            pd.Series: The colum with the accumulated index by year
        """
        idx = (row / 100) + 1
        return (np.cumprod(idx) - 1) * 100
