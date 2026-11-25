"""
Accent removal, special character removal, and lowercasing.
Equivalent of fnc/clean_string.do.
"""

import re

# Pre-built translation table matching the exact Stata replacements.
# Stata replaces accented chars with their base letter, then removes
# specific punctuation, then lowercases.

_ACCENT_MAP = str.maketrans(
    {
        # lowercase
        "á": "a",
        "à": "a",
        "â": "a",
        "ã": "a",
        "ä": "a",
        "é": "e",
        "è": "e",
        "ê": "e",
        "ë": "e",
        "í": "i",
        "ì": "i",
        "î": "i",
        "ï": "i",
        "ó": "o",
        "ò": "o",
        "ô": "o",
        "õ": "o",
        "ö": "o",
        "ú": "u",
        "ù": "u",
        "ü": "u",
        "ç": "c",
        "ñ": "n",
        # uppercase
        "Á": "A",
        "À": "A",
        "Â": "A",
        "Ã": "A",
        "Ä": "A",
        "É": "E",
        "È": "E",
        "Ê": "E",
        "Ë": "E",
        "Í": "I",
        "Ì": "I",
        "Î": "I",
        "Ï": "I",
        "Ó": "O",
        "Ò": "O",
        "Ô": "O",
        "Õ": "O",
        "Ö": "O",
        "Ú": "U",
        "Ù": "U",
        "Ü": "U",
        "Ç": "C",
        "Ñ": "N",
    }
)

_REMOVE_CHARS = str.maketrans("", "", ".,[]{}'\u0060")  # backtick = \u0060


def clean_string(val: str) -> str:
    """
    Clean a single string value:
    1. Replace accented characters with ASCII base letters
    2. Replace underscores with spaces
    3. Remove . , [ ] { } ' `
    4. Collapse double spaces, strip
    5. Lowercase
    """
    if not val:
        return val
    val = val.translate(_ACCENT_MAP)
    val = val.replace("_", " ")
    val = val.translate(_REMOVE_CHARS)
    # Collapse multiple spaces
    val = re.sub(r"  +", " ", val)
    val = val.strip()
    val = val.lower()
    return val


def clean_string_series(s):
    """Apply clean_string to a pandas Series."""
    return s.map(lambda v: clean_string(v) if isinstance(v, str) else v)
