"""
Election type standardization.
Equivalent of fnc/limpa_tipo_eleicao.do.
"""


def clean_election_type(val: str, ano: int) -> str:
    """Standardize tipo_eleicao to 'eleicao ordinaria' for known variants."""
    if not val:
        return "eleicao ordinaria"

    ano_s = str(ano)
    ordinaria_variants = {
        "",
        "ordinaria",
        f"eleicoes ordinarias - {ano_s}",
        f"eleicoes {ano_s}",
        f"eleicao municipal {ano_s}",
        f"eleicoes gerais {ano_s}",
        f"eleicoes gerais {ano_s} primeiro turno",
        f"eleicoes gerais {ano_s} segundo turno",
        f"eleicoes municipais de {ano_s} - 1. turno",
        f"eleicoes municipais de {ano_s} - 2. turno",
        f"eleicoes municipais de {ano_s} - 1 turno",
        f"eleicoes municipais de {ano_s} - 2 turno",
        f"eleicoes municipais de {ano_s} - 1\u00ba turno",
        f"eleicoes municipais de {ano_s} - 2\u00ba turno",
        f"eleicoes municipais {ano_s}",
        f"eleicoes gerais estaduais {ano_s}",
        f"eleicao geral federal {ano_s}",
    }
    # Map "eleicao YYYY" → "eleicao ordinaria" for all years.
    ordinaria_variants.add(f"eleicao {ano_s}")

    if val in ordinaria_variants:
        return "eleicao ordinaria"
    return val


def clean_election_type_series(s, ano: int):
    """Apply clean_election_type to a pandas Series."""
    return s.map(
        lambda v: clean_election_type(v, ano) if isinstance(v, str) else v
    )
