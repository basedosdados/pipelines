"""List Prefect 3 deployments with their armed/paused state.

The databasis MCP exposes flow *runs* but nothing that enumerates
deployments, so "which pipelines are currently disarmed?" — the question
behind the team's deactivate-and-move-on workflow — has no MCP answer.
This fills that gap and nothing else.

    uv run python .claude/skills/pipeline-doctor/scripts/deployments.py
    uv run python .claude/skills/pipeline-doctor/scripts/deployments.py --paused
    uv run python .claude/skills/pipeline-doctor/scripts/deployments.py --json

Reads the prefect3 token from ~/.basedosdados/credentials.json and never
prints it. Same token the MCP server uses; see `_prefect_key` in
~/Dropbox/BD/mcp/server.py for why it is a backend Token, not a Prefect key.

`paused` is the authoritative arming signal. A paused deployment still
reports `active=True` on its schedule — deployment-level paused wins — so
read `paused`, never the schedule flag.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys

import requests

API = "https://prefect3.basedosdados.org/api"
PAGE = 200


def _token() -> str:
    path = pathlib.Path.home() / ".basedosdados" / "credentials.json"
    if not path.exists():
        sys.exit(f"missing {path}; cannot reach Prefect")
    key = json.loads(path.read_text()).get("prod", {}).get("prefect3")
    if not key:
        sys.exit(
            "no 'prefect3' key under 'prod' in ~/.basedosdados/credentials.json. "
            "It is a backend Token scoped to the prefect3.basedosdados.org "
            "domain; the older 'prefect' key is the retired Prefect 2 host."
        )
    return key


def _post_paged(path: str, body: dict) -> list[dict]:
    """POST a filter endpoint, walking Prefect's 200-row per-request cap."""
    head = {"Authorization": f"Bearer {_token()}"}
    out: list[dict] = []
    while True:
        r = requests.post(
            f"{API}{path}",
            json={**body, "limit": PAGE, "offset": len(out)},
            headers=head,
            timeout=60,
        )
        if not r.ok:
            sys.exit(f"HTTP {r.status_code} from {path}: {r.text[:300]}")
        page = r.json()
        out.extend(page)
        if len(page) < PAGE:
            return out


def collect() -> list[dict]:
    """One row per deployment: flow, pool, arming state, cron."""
    deployments = _post_paged("/deployments/filter", {})
    flow_ids = list({d["flow_id"] for d in deployments if d.get("flow_id")})
    names: dict[str, str] = {}
    for i in range(0, len(flow_ids), PAGE):
        chunk = flow_ids[i : i + PAGE]
        r = requests.post(
            f"{API}/flows/filter",
            json={"flows": {"id": {"any_": chunk}}, "limit": len(chunk)},
            headers={"Authorization": f"Bearer {_token()}"},
            timeout=60,
        )
        r.raise_for_status()
        names.update({f["id"]: f["name"] for f in r.json()})

    rows = []
    for d in deployments:
        flow_id = str(d.get("flow_id") or "")
        crons = [
            s.get("schedule", {}).get("cron")
            for s in (d.get("schedules") or [])
            if s.get("schedule", {}).get("cron")
        ]
        rows.append(
            {
                "flow": names.get(flow_id, "?"),
                "deployment": d.get("name"),
                "pool": d.get("work_pool_name"),
                "paused": bool(d.get("paused")),
                "crons": crons,
                "version": d.get("version"),
                "updated": d.get("updated"),
                "id": d.get("id"),
            }
        )
    return sorted(rows, key=lambda r: (r["pool"] or "", r["flow"]))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--pool", help="filter by work pool substring")
    ap.add_argument(
        "--paused", action="store_true", help="only disarmed deployments"
    )
    ap.add_argument("--json", action="store_true", help="machine-readable")
    args = ap.parse_args()

    rows = collect()
    if args.pool:
        rows = [r for r in rows if args.pool in (r["pool"] or "")]
    if args.paused:
        rows = [r for r in rows if r["paused"]]

    if args.json:
        print(json.dumps(rows, indent=2))
        return 0

    print(f"{len(rows)} deployment(s)\n")
    print(f"{'STATE':<8} {'POOL':<20} {'DEPLOYMENT':<48} CRON")
    for r in rows:
        state = "PAUSED" if r["paused"] else "armed"
        cron = "; ".join(r["crons"]) or "(none)"
        print(
            f"{state:<8} {(r['pool'] or '-'):<20} "
            f"{(r['deployment'] or '-'):<48} {cron}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
