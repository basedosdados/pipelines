"""Move auxiliary-file bundles to the public bucket and repoint the metadata.

`gs://basedosdados` and `gs://basedosdados-dev` are both requester-pays, so every
`Table.auxiliaryFilesUrl` served from them returns HTTP 400 `UserProjectMissing`
to an anonymous visitor. `gs://basedosdados-public` is not requester-pays and is
already how one-click table downloads reach the public, so the bundles move there.

Three phases, each separately runnable and each idempotent. Run them in order:

    copy    replicate the referenced auxiliary_files/** objects to the public bucket
    rewrite repoint every Table.auxiliaryFilesUrl at the public bucket
    verify  fetch every registered URL with no credentials and report the status

`copy` and `rewrite` default to a dry run; pass --apply to make changes.

    python .github/scripts/migrate_auxiliary_files.py copy   --env prod
    python .github/scripts/migrate_auxiliary_files.py copy   --env prod --apply
    python .github/scripts/migrate_auxiliary_files.py rewrite --env prod --token "$TOKEN"
    python .github/scripts/migrate_auxiliary_files.py rewrite --env prod --token "$TOKEN" --apply
    python .github/scripts/migrate_auxiliary_files.py verify  --env prod

Credentials:

- `copy` needs read on both source buckets and write on the public bucket. The
  local dev service account has neither, so run it with prod credentials.
- `rewrite` needs a backend token, for reads as well as writes -- see the note on
  `fetch_tables`.
- `verify` deliberately uses none: it measures what a site visitor gets.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request

PREFIX = "auxiliary_files/"
SOURCE_BUCKETS = ("basedosdados", "basedosdados-dev")
TARGET_BUCKET = "basedosdados-public"
PUBLIC_ROOT = f"https://storage.googleapis.com/{TARGET_BUCKET}/"

BACKENDS = {
    "prod": "https://backend.basedosdados.org",
    "staging": "https://staging.backend.basedosdados.org",
    "dev": "https://development.backend.basedosdados.org",
}

# Billing project charged for reads against the requester-pays source buckets.
BILLING_PROJECT = "basedosdados"


# ---------------------------------------------------------------- storage ---


def _storage_client():
    from google.cloud import storage

    return storage.Client(project=BILLING_PROJECT)


def list_source_objects(client) -> dict[str, list]:
    """Relative path -> [blob, ...], newest-looking source last.

    Both source buckets are requester-pays, so every bucket handle carries a
    `user_project`. Directory placeholder objects (trailing slash) are skipped.
    """
    found: dict[str, list] = {}
    for name in SOURCE_BUCKETS:
        bucket = client.bucket(name, user_project=BILLING_PROJECT)
        for blob in client.list_blobs(bucket, prefix=PREFIX):
            if blob.name.endswith("/"):
                continue
            found.setdefault(blob.name, []).append(blob)
    return found


def referenced_paths(backend: str) -> set[str]:
    """Object paths that some table's auxiliaryFilesUrl actually points at.

    The source buckets also hold unreferenced scratch -- `auxiliary_files/bla/
    bla/data.csv`, stray font files. Copying by reference keeps that out of the
    public bucket rather than laundering it into a world-readable location.
    """
    paths = set()
    for url in registered_urls(backend):
        for bucket in SOURCE_BUCKETS:
            root = f"https://storage.googleapis.com/{bucket}/"
            if url.startswith(root):
                paths.add(url[len(root) :])
    return paths


def copy_objects(backend: str, apply: bool, everything: bool) -> int:
    client = _storage_client()
    target = client.bucket(TARGET_BUCKET)
    sources = list_source_objects(client)
    print(
        f"{len(sources)} object(s) under {PREFIX} across {', '.join(SOURCE_BUCKETS)}"
    )

    if not everything:
        wanted = referenced_paths(backend)
        missing = sorted(wanted - sources.keys())
        sources = {k: v for k, v in sources.items() if k in wanted}
        print(
            f"{len(wanted)} referenced by a registered auxiliaryFilesUrl; "
            f"{len(sources)} of them exist"
        )
        if missing:
            print(
                f"\n{len(missing)} referenced path(s) exist in neither source bucket:"
            )
            for path in missing:
                print(f"  {path}")
            print("Those links are already broken; copying cannot fix them.")

    conflicts = {
        p: bs for p, bs in sources.items() if len({b.md5_hash for b in bs}) > 1
    }
    if conflicts:
        print(
            f"\nABORT: {len(conflicts)} path(s) differ between source buckets:"
        )
        for path, blobs in conflicts.items():
            detail = ", ".join(f"{b.bucket.name}={b.size}B" for b in blobs)
            print(f"  {path}  ({detail})")
        print("Resolve by hand before copying — this script will not guess.")
        return 1

    copied = skipped = 0
    for path, blobs in sorted(sources.items()):
        source = blobs[-1]
        existing = target.get_blob(path)
        if existing is not None and existing.md5_hash == source.md5_hash:
            skipped += 1
            continue
        verb = "copy" if apply else "would copy"
        print(
            f"  {verb}  gs://{source.bucket.name}/{path}  ->  gs://{TARGET_BUCKET}/{path}"
        )
        if apply:
            source.bucket.copy_blob(source, target, new_name=path)
        copied += 1

    print(
        f"\n{'copied' if apply else 'to copy'}: {copied}   already present: {skipped}"
    )
    if not apply:
        print("Dry run — pass --apply to write.")
    return 0


# ---------------------------------------------------------------- backend ---


def graphql(
    backend: str, query: str, variables: dict, token: str | None = None
) -> dict:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(
        f"{backend}/api/v1/graphql",
        data=json.dumps({"query": query, "variables": variables}).encode(),
        headers=headers,
    )
    payload = json.loads(urllib.request.urlopen(request, timeout=120).read())
    if payload.get("errors"):
        raise RuntimeError(payload["errors"])
    return payload["data"]


# Every writable field on CreateUpdateTableInput except clientMutationId, split by
# shape. The mutation binds a Django ModelForm with `data=input`, so it is a full
# replace: any field left out is treated as empty and silently cleared. Every
# update therefore reads the current values and sends them all back.
SCALAR_FIELDS = (
    "slug",
    "name",
    "namePt",
    "nameEn",
    "nameEs",
    "description",
    "descriptionPt",
    "descriptionEn",
    "descriptionEs",
    "version",
    "isDeprecated",
    "isDirectory",
    "isClosed",
    "dataCleaningDescription",
    "dataCleaningCodeUrl",
    "auxiliaryFilesUrl",
    "architectureUrl",
    "sourceBucketName",
    "uncompressedFileSize",
    "compressedFileSize",
    "numberRows",
    "numberColumns",
    "pageViews",
)
FK_FIELDS = ("dataset", "status", "license", "partnerOrganization", "pipeline")
M2M_FIELDS = ("rawDataSource", "publishedBy", "dataCleanedBy")

_SCALARS = " ".join(SCALAR_FIELDS)
_FKS = " ".join(f"{f} {{ id }}" for f in FK_FIELDS)
_M2MS = " ".join(f"{f} {{ edges {{ node {{ id }} }} }}" for f in M2M_FIELDS)

TABLES_QUERY = f"""
query($after: String) {{
  allTable(first: 200, after: $after) {{
    pageInfo {{ hasNextPage endCursor }}
    edges {{ node {{ id {_SCALARS} {_FKS} {_M2MS} }} }}
  }}
}}
"""


def pk(global_id: str) -> str:
    """`TableNode:<uuid>` -> `<uuid>`; the mutation wants the bare primary key."""
    return global_id.split(":", 1)[-1]


def fetch_tables(backend: str, token: str) -> list[dict]:
    """Every table, cursor-paginated, read with the same token used to write.

    `first: N` silently truncates and still reports `hasNextPage: true`, so the
    loop runs to exhaustion rather than trusting a single large page.

    `publishedBy` and `dataCleanedBy` are account references and are refused to
    anonymous callers. `graphql` raises on any error, including a partial one, so
    a token that cannot read them stops the run instead of quietly yielding None
    and letting the full-replace mutation clear them.
    """
    after, rows = None, []
    while True:
        block = graphql(backend, TABLES_QUERY, {"after": after}, token)[
            "allTable"
        ]
        rows.extend(edge["node"] for edge in block["edges"])
        if not block["pageInfo"]["hasNextPage"]:
            return rows
        after = block["pageInfo"]["endCursor"]


def target_url(url: str) -> str | None:
    """Rewrite a requester-pays GCS URL onto the public bucket, else None.

    The path under `auxiliary_files/` is preserved, so a row registered against
    the wrong source bucket (world_oecd_piaac points at prod while its bundles
    sit in dev) lands on the same public object as everything else.
    """
    for bucket in SOURCE_BUCKETS:
        root = f"https://storage.googleapis.com/{bucket}/{PREFIX}"
        if url.startswith(root):
            return PUBLIC_ROOT + PREFIX + url[len(root) :]
    return None


def build_input(table: dict, new_url: str) -> dict:
    """Full replacement payload: every current value, with the URL swapped."""
    payload: dict = {"id": pk(table["id"])}
    for field in SCALAR_FIELDS:
        value = table.get(field)
        if value is not None:
            payload[field] = value
    for field in FK_FIELDS:
        node = table.get(field)
        if node:
            payload[field] = pk(node["id"])
    for field in M2M_FIELDS:
        edges = (table.get(field) or {}).get("edges") or []
        if edges:
            payload[field] = [pk(e["node"]["id"]) for e in edges]
    payload["auxiliaryFilesUrl"] = new_url
    return payload


def rewrite_urls(backend: str, token: str | None, apply: bool) -> int:
    if not token:
        print(
            "ABORT: --token is required.\n"
            "The mutation is a full replace, so every field has to be read back and\n"
            "re-sent. publishedBy and dataCleanedBy are not readable anonymously, and\n"
            "a dry run built from a partial read would not match what --apply writes."
        )
        return 1

    tables = fetch_tables(backend, token)
    pending = []
    for table in tables:
        url = table.get("auxiliaryFilesUrl")
        if not url:
            continue
        new = target_url(url)
        if new and new != url:
            pending.append((table, url, new))

    distinct = {old for _, old, _ in pending}
    print(
        f"{len(tables)} table(s); {len(pending)} row(s) to repoint "
        f"across {len(distinct)} distinct URL(s)"
    )

    if not apply:
        for table, old, new in pending:
            print(f"  would repoint {table['slug']}\n      {old}\n   -> {new}")
        print("\nDry run — pass --apply to write.")
        return 0

    mutation = """
    mutation($input: CreateUpdateTableInput!) {
      CreateUpdateTable(input: $input) { ok errors { field messages } }
    }
    """
    failures = 0
    for table, _old, new in pending:
        result = graphql(
            backend, mutation, {"input": build_input(table, new)}, token
        )["CreateUpdateTable"]
        if result["ok"]:
            print(f"  ok      {table['slug']}  -> {new}")
        else:
            failures += 1
            print(f"  FAILED  {table['slug']}  {result['errors']}")
    print(f"\nrepointed: {len(pending) - failures}   failed: {failures}")
    return 1 if failures else 0


# ----------------------------------------------------------------- verify ---


URLS_QUERY = """
query($after: String) {
  allTable(first: 200, after: $after) {
    pageInfo { hasNextPage endCursor }
    edges { node { auxiliaryFilesUrl } }
  }
}
"""


def registered_urls(backend: str) -> set[str]:
    """Every non-empty auxiliaryFilesUrl. Reads only the URL, so no token needed."""
    after, urls = None, set()
    while True:
        block = graphql(backend, URLS_QUERY, {"after": after})["allTable"]
        urls.update(
            edge["node"]["auxiliaryFilesUrl"]
            for edge in block["edges"]
            if edge["node"].get("auxiliaryFilesUrl")
        )
        if not block["pageInfo"]["hasNextPage"]:
            return urls
        after = block["pageInfo"]["endCursor"]


def verify(backend: str) -> int:
    """Fetch every registered auxiliaryFilesUrl anonymously and report the status.

    This is the check the rule asks for: no credentials, no billing project —
    exactly what a site visitor gets.
    """
    urls = sorted(registered_urls(backend))
    bad = 0
    for url in urls:
        request = urllib.request.Request(url, method="HEAD")
        try:
            code = urllib.request.urlopen(request, timeout=60).status
        except urllib.error.HTTPError as exc:
            code = exc.code
        except Exception as exc:
            code = repr(exc)
        ok = code == 200
        bad += 0 if ok else 1
        print(f"  {'OK ' if ok else 'BAD'}  {code}  {url}")
    print(f"\n{len(urls) - bad}/{len(urls)} resolve anonymously")
    return 1 if bad else 0


# ------------------------------------------------------------------- main ---


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("phase", choices=("copy", "rewrite", "verify"))
    parser.add_argument("--env", choices=tuple(BACKENDS), default="prod")
    parser.add_argument(
        "--token", help="backend JWT; required for the rewrite phase"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="make changes (default is a dry run)",
    )
    parser.add_argument(
        "--everything",
        action="store_true",
        help="copy phase: copy every auxiliary_files/ object, not only referenced ones",
    )
    args = parser.parse_args()

    if args.phase == "copy":
        return copy_objects(BACKENDS[args.env], args.apply, args.everything)
    if args.phase == "rewrite":
        return rewrite_urls(BACKENDS[args.env], args.token, args.apply)
    return verify(BACKENDS[args.env])


if __name__ == "__main__":
    sys.exit(main())
