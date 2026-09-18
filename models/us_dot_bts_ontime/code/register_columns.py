"""Register the us_dot_bts_ontime columns from the architecture CSVs.

    uv run --no-project --python 3.11 --with fastmcp --with requests \
        python models/us_dot_bts_ontime/code/register_columns.py staging

Drives the databasis MCP's `bulk_upsert_columns` in-process rather than through a
tool call, because the `flight` payload is 53 kB of JSON and there is no reason to
push that through the conversation. The MCP module is imported by path, the same
way `world_oecd_piaac` does it.

`bulk_upsert_columns` matches columns by NAME and writes only the fields present in
each row, so re-running is safe: it never blanks a field it was not given and never
clobbers the partition flag.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from gen_columns_json import payload  # noqa: E402

MCP_SERVER = Path.home() / "Dropbox" / "BD" / "mcp" / "server.py"

TABLE_IDS = {
    "flight": "245b6498-5295-44df-8f4d-62496f2ba898",
    "airport": "2a0d9769-c080-4a84-a77e-016996a3fae8",
    "dicionario": "fe828676-962b-4354-9605-fb83acd2b3b3",
}


def load_mcp():
    spec = importlib.util.spec_from_file_location(
        "databasis_mcp_server", MCP_SERVER
    )
    if spec is None or spec.loader is None:
        raise SystemExit(f"cannot import the databasis MCP at {MCP_SERVER}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def call(tool):
    """FastMCP wraps each tool; the plain callable hangs off `.fn`."""
    return getattr(tool, "fn", tool)


def main(env: str, tables: list[str]) -> None:
    mcp = load_mcp()
    call(mcp.auth)(env=env)
    for table in tables:
        js = payload(table)
        result = call(mcp.bulk_upsert_columns)(
            table_id=TABLE_IDS[table], columns_json=js, env=env
        )
        created = result.get("created", result.get("n_created"))
        updated = result.get("updated", result.get("n_updated"))
        print(
            f"{table}: {len(js):,} bytes -> created={created} updated={updated}"
        )
        errors = result.get("errors") or result.get("failed")
        if errors:
            print(f"  ERRORS: {errors}")
            raise SystemExit(1)


if __name__ == "__main__":
    argv = sys.argv[1:]
    env = argv[0] if argv else "staging"
    main(env, argv[1:] or list(TABLE_IDS))
