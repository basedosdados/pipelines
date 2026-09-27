"""
Marital status normalization.
Equivalent of fnc/limpa_estado_civil.do.
"""

_MAP = {
    "solteiro": "solteiro(a)",
    "casado": "casado(a)",
    "divorciado": "divorciado(a)",
    "separado(a) judicialmente": "divorciado(a)",
    "separado judicialmente": "divorciado(a)",
    "viuvo": "viuvo(a)",
    "nao divulgavel": "",
    "nao informado": "",
}


def clean_marital_status(val: str) -> str:
    return _MAP.get(val, val)


def clean_marital_status_series(s):
    """Apply clean_marital_status to a pandas Series."""
    return s.map(
        lambda v: clean_marital_status(v) if isinstance(v, str) else v
    )
