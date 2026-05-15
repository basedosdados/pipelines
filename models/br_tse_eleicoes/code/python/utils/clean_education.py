"""
Education level normalization.
Equivalent of fnc/limpa_instrucao.do.
"""

_MAP = {
    "1º grau incompleto": "ensino fundamental incompleto",
    "primeiro grau incompleto": "ensino fundamental incompleto",
    "fundamental incompleto": "ensino fundamental incompleto",
    "1º grau completo": "ensino fundamental completo",
    "primeiro grau completo": "ensino fundamental completo",
    "fundamental completo": "ensino fundamental completo",
    "2º grau completo": "ensino medio completo",
    "2º grau incompleto": "ensino medio incompleto",
    "segundo grau completo": "ensino medio completo",
    "segundo grau incompleto": "ensino medio incompleto",
    "medio completo": "ensino medio completo",
    "medio incompleto": "ensino medio incompleto",
    "superior completo": "ensino superior completo",
    "superior incompleto": "ensino superior incompleto",
    "nao divulgavel": "",
    "nao informado": "",
    "informacao nao recuperada": "",
}


def clean_education(val: str) -> str:
    return _MAP.get(val, val)


def clean_education_series(s):
    """Apply clean_education to a pandas Series."""
    return s.map(lambda v: clean_education(v) if isinstance(v, str) else v)
