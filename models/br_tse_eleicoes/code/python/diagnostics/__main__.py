"""CLI for the schema diagnostics harness.

Run from ``models/br_tse_eleicoes/code/python``:

    TSE_DATA_DIR=/tmp/dados_TSE uv run --with pdfplumber python -m diagnostics run --tier 1
    ... run --tier 2 [--force]
    ... run --tier 3 [--table candidatos]
    ... report
    ... status
"""

from __future__ import annotations

import argparse
import sys


def main(argv=None):
    parser = argparse.ArgumentParser(prog="diagnostics")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="run a tier")
    p_run.add_argument("--tier", type=int, choices=(1, 2, 3), required=True)
    p_run.add_argument("--table", default="all")
    p_run.add_argument("--force", action="store_true")

    sub.add_parser("report", help="regenerate DIAGNOSIS.md sections")
    sub.add_parser("status", help="print summary from last findings")

    args = parser.parse_args(argv)

    if args.cmd == "run" and args.tier == 1:
        from . import tier1_audit

        audits = tier1_audit.run(args.table)
        print(tier1_audit.summarize(audits))
        n_issues = sum(len(a.issues) for a in audits)
        return 1 if n_issues else 0

    if args.cmd == "run" and args.tier == 2:
        from . import tier2_leiame

        results = tier2_leiame.acquire_all(
            tier2_leiame.needed_pairs(args.table), force=args.force
        )
        missing = [k for k, v in results.items() if v is None]
        if missing:
            print(f"\n{len(missing)} layouts NOT FOUND: {missing}")
        return 1 if missing else 0

    if args.cmd == "run" and args.tier == 3:
        from . import tier3_check

        findings = tier3_check.run(args.table)
        print(tier3_check.summarize(findings))
        return 1 if any(f.severity == "FAIL" for f in findings) else 0

    if args.cmd == "report":
        from . import report

        report.update_diagnosis_md()
        return 0

    if args.cmd == "status":
        import json

        from .report import FINDINGS_PATH, _matrix

        findings = json.loads(FINDINGS_PATH.read_text())
        print(_matrix(findings))
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
