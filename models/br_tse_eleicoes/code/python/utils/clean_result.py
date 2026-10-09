"""
Election result normalization.
Equivalent of fnc/limpa_resultado.do.
"""

_MAP = {
    "media": "eleito por media",
    "eleito por quociente partidario": "eleito por qp",
    "renuncia/falecimento com substituicao": "renuncia/falecimento/cassacao",
    "renuncia/falecimento/cassacao antes da eleicao": "renuncia/falecimento/cassacao",
    "renuncia;falecimento;cassacao antes da eleicao": "renuncia/falecimento/cassacao",
    "renuncia;falecimento;cassacao apos a eleicao": "renuncia/falecimento/cassacao",
    "2o turno": "2º turno",
}


def clean_result(val: str) -> str:
    return _MAP.get(val, val)


def clean_result_series(s):
    """Apply clean_result to a pandas Series."""
    return s.map(lambda v: clean_result(v) if isinstance(v, str) else v)
