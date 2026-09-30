"""Find which waves encode CH15_COD / CH16_COD alphabetically rather than numerically.

CH15_COD (place of birth) and CH16_COD (residence five years ago) are not merely
padded differently across eras -- they use two incompatible encodings. Most waves
carry INDEC's numeric province/country codes, but some carry three-letter
abbreviations ("tuc", "bol", "par"), in inconsistent case. The two cannot be
compared without a crosswalk, so the affected waves are identified here and
recorded in the column's observations rather than silently merged.
"""

import json
import shutil
import sys
import tempfile
from pathlib import Path

import pandas as pd
import pyreadstat

sys.path.insert(0, str(Path(__file__).resolve().parent))
from archives import data_members, extract
from constants import CODE_DIR, waves

COLUMNS = ["CH15_COD", "CH16_COD"]


def main() -> int:
    result: dict[str, dict[str, dict]] = {c: {} for c in COLUMNS}
    for wave in waves():
        tag = f"{wave['year']}Q{wave['quarter']}"
        member = data_members(wave)["microdatos_individuo"]
        tmp = Path(tempfile.mkdtemp(prefix="eph_geo_"))
        try:
            path = extract(wave, member, tmp)
            if wave["fmt"] == "dta":
                frame, meta = pyreadstat.read_dta(str(path))
                frame.columns = [c.upper() for c in meta.column_names]
                frame = frame.astype("string")
            else:
                frame = pd.read_csv(
                    path,
                    sep=";",
                    encoding="latin-1",
                    dtype=str,
                    low_memory=False,
                )
                frame.columns = [
                    str(c).strip().strip('"').upper() for c in frame.columns
                ]
                frame = frame.astype("string")
            for col in COLUMNS:
                if col not in frame.columns:
                    continue
                s = (
                    frame[col]
                    .str.strip()
                    .replace({"": pd.NA, "nan": pd.NA})
                    .dropna()
                )
                if s.empty:
                    continue
                alpha = int(s.str.contains(r"[A-Za-z]").sum())
                result[col][tag] = {
                    "n": len(s),
                    "alphabetic": alpha,
                    "share_alphabetic": round(alpha / len(s), 4),
                }
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
        print(f"scanned {tag}", flush=True)

    (CODE_DIR / "geo_code_encoding.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    for col in COLUMNS:
        affected = [
            t for t, v in result[col].items() if v["share_alphabetic"] > 0.5
        ]
        print(f"\n{col}: {len(affected)} waves are predominantly alphabetic")
        print(f"   {affected}")
        mixed = [
            t
            for t, v in result[col].items()
            if 0 < v["share_alphabetic"] <= 0.5
        ]
        if mixed:
            print(f"   partially alphabetic: {mixed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
