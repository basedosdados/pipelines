"""Tier 3 — cross-check code mappings (tier 1) vs official layouts (tier 2).

For every mapping site x year:
  * positional sites: official variable at position N must be affinity-
    compatible with the BD column the code maps ``vN`` to;
  * named sites: every TSE header key the code selects must exist in the
    official layout.

Findings are classified FAIL / WARN / OK / NO_LAYOUT and written to
``artifacts/findings.json`` for the report generator.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

from . import tier1_audit
from .affinity import compatible
from .spec import SUPPRESSED_SITES
from .tier2_leiame import load_layout

ARTIFACTS = Path(__file__).resolve().parent / "artifacts"
FINDINGS_PATH = ARTIFACTS / "findings.json"


@dataclass
class Finding:
    table: str
    module: str
    function: str
    lineno: int
    family: str
    ano: int
    severity: str  # FAIL | WARN | OK | NO_LAYOUT
    kind: str  # POSITIONAL_MAPPING_MISMATCH | OUT_OF_RANGE | MISSING_KEY | OK | NO_LAYOUT
    layout_source: str = ""
    variant: str = ""
    details: list[dict] = field(default_factory=list)


def _check_positional(site, layout) -> tuple[list[dict], list[dict]]:
    """Return (mismatches, out_of_range) for a positional site x layout."""
    mismatches, out_of_range = [], []
    for v, bd_col in site["mapping"].items():
        if bd_col == v:
            # temporary passthrough column (e.g. detalhe 1994/98 keeps
            # v43-v45 and sums them later) — nothing to judge by name
            continue
        pos = int(v[1:])
        if pos > len(layout.columns):
            out_of_range.append(
                {"v": v, "bd": bd_col, "layout_len": len(layout.columns)}
            )
            continue
        tse = layout.columns[pos - 1]
        if not compatible(bd_col, tse):
            mismatches.append({"v": v, "bd": bd_col, "tse": tse})
    return mismatches, out_of_range


def _check_named(site, layout) -> list[dict]:
    cols = {c.lower() for c in layout.columns}
    return [
        {"key": k, "bd": v}
        for k, v in site["mapping"].items()
        if k.lower() not in cols
    ]


def run(table: str | None = None) -> list[Finding]:
    audits = tier1_audit.run(table)
    findings: list[Finding] = []

    for audit in audits:
        table_name = audit.tables[0]
        for site in audit.sites:
            site_d = asdict(site) if not isinstance(site, dict) else site
            suppressed = SUPPRESSED_SITES.get((audit.module, site_d["lineno"]))
            for ano in site_d["years"]:
                if suppressed:
                    findings.append(
                        Finding(
                            table=table_name,
                            module=audit.module,
                            function=audit.function,
                            lineno=site_d["lineno"],
                            family=audit.family,
                            ano=ano,
                            severity="WARN",
                            kind="UNVERIFIED_VARIANT",
                            variant=site_d.get("variant", ""),
                            details=[{"reason": suppressed}],
                        )
                    )
                    continue
                layout = load_layout(audit.family, ano)
                base = dict(
                    table=table_name,
                    module=audit.module,
                    function=audit.function,
                    lineno=site_d["lineno"],
                    family=audit.family,
                    ano=ano,
                    variant=site_d.get("variant", ""),
                )
                if layout is None or len(layout.columns) < 3:
                    findings.append(
                        Finding(**base, severity="NO_LAYOUT", kind="NO_LAYOUT")
                    )
                    continue
                base["layout_source"] = layout.source.split(":", 1)[0]

                if site_d["kind"] == "named":
                    missing = _check_named(site_d, layout)
                    if missing:
                        # named selection cannot misalign — a missing key
                        # only drops the column, so this is WARN not FAIL
                        findings.append(
                            Finding(
                                **base,
                                severity="WARN",
                                kind="MISSING_KEY",
                                details=missing,
                            )
                        )
                    else:
                        findings.append(
                            Finding(**base, severity="OK", kind="OK")
                        )
                    continue

                mismatches, out_of_range = _check_positional(site_d, layout)
                if out_of_range:
                    findings.append(
                        Finding(
                            **base,
                            severity="FAIL" if layout.has_header else "WARN",
                            kind="OUT_OF_RANGE",
                            details=out_of_range,
                        )
                    )
                if mismatches:
                    findings.append(
                        Finding(
                            **base,
                            severity="FAIL" if layout.has_header else "WARN",
                            kind="POSITIONAL_MAPPING_MISMATCH",
                            details=mismatches,
                        )
                    )
                if not mismatches and not out_of_range:
                    findings.append(Finding(**base, severity="OK", kind="OK"))

    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    FINDINGS_PATH.write_text(
        json.dumps([asdict(f) for f in findings], indent=1, ensure_ascii=False)
    )
    return findings


def summarize(findings: list[Finding]) -> str:
    by_sev: dict[str, int] = {}
    for f in findings:
        by_sev[f.severity] = by_sev.get(f.severity, 0) + 1
    lines = [
        "tier3: " + ", ".join(f"{k}={v}" for k, v in sorted(by_sev.items()))
    ]
    for f in findings:
        if f.severity in ("FAIL", "WARN"):
            lines.append(
                f"  {f.severity} {f.table} {f.ano} "
                f"[{f.module}::{f.function} L{f.lineno}] {f.kind} "
                f"({f.layout_source or 'no layout'})"
            )
            for d in f.details[:12]:
                if "tse" in d:
                    lines.append(
                        f"    {d['v']} -> {d['bd']}  but official[{d['v'][1:]}] = {d['tse']}"
                    )
                elif "key" in d:
                    lines.append(
                        f"    key {d['key']} (-> {d['bd']}) not in layout"
                    )
                else:
                    lines.append(f"    {d}")
            if len(f.details) > 12:
                lines.append(f"    ... +{len(f.details) - 12} more")
    return "\n".join(lines)


if __name__ == "__main__":
    print(summarize(run()))
