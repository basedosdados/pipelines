import polars as pl
import pandas as pd
import os
from utils import guess_header, change_df_header, get_year_month_from_filename


DICT_UFS = {
    "AC": "Acre",
    "AL": "Alagoas",
    "AP": "Amapá",
    "AM": "Amazonas",
    "BA": "Bahia",
    "CE": "Ceará",
    "DF": "Distrito Federal",
    "ES": "Espírito Santo",
    "GO": "Goiás",
    "MA": "Maranhão",
    "MT": "Mato Grosso",
    "MS": "Mato Grosso do Sul",
    "MG": "Minas Gerais",
    "PA": "Pará",
    "PB": "Paraíba",
    "PR": "Paraná",
    "PE": "Pernambuco",
    "PI": "Piauí",
    "RJ": "Rio de Janeiro",
    "RN": "Rio Grande do Norte",
    "RS": "Rio Grande do Sul",
    "RO": "Rondônia",
    "RR": "Roraima",
    "SC": "Santa Catarina",
    "SP": "São Paulo",
    "SE": "Sergipe",
    "TO": "Tocantins",
}


def treat_uf_tipo(file) -> pl.DataFrame:
    filename = os.path.split(file)[1]
    df = pd.read_excel(file)
    new_df = change_df_header(df, guess_header(df))
    # This is ad hoc for UF_tipo.
    new_df.rename(
        columns={new_df.columns[0]: "sigla_uf"}, inplace=True
    )  # Rename for ease of use.
    new_df.sigla_uf = new_df.sigla_uf.str.strip()  # Remove whitespace.
    clean_df = new_df[new_df.sigla_uf.isin(DICT_UFS.values())].reset_index(
        drop=True
    )  # Now we get all the actual RELEVANT uf data.
    month, year = get_year_month_from_filename(filename)
    clean_pl_df = pl.from_pandas(clean_df).lazy()
    # Add year and month
    clean_pl_df = clean_pl_df.with_columns(
        pl.lit(year, dtype=pl.Int64).alias("ano"),
        pl.lit(month, dtype=pl.Int64).alias("mes"),
    )
    clean_pl_df = clean_pl_df.melt(
        id_vars=["ano", "mes", "sigla_uf"],
        variable_name="tipo_veiculo",
        value_name="quantidade",
    )  # Long format.
    return clean_pl_df.collect()
