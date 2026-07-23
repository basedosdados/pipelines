"""Resilient upload for the big microdata_person table.

The 30 GB monolithic upload keeps dying on SSL drops mid-file, and each retry
restarts from scratch. This uploads files to GCS staging with if_exists="pass"
(skip files already there) inside a retry loop, so progress ACCUMULATES across
retries. Then creates the BQ table over the uploaded data (no re-upload).
"""

import time
import warnings

warnings.filterwarnings("ignore")
import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
from google.cloud import bigquery  # noqa: E402

BILLING = "basedosdados-dev"
DATASET = "us_census_acs"
TABLE = "microdata_person"
PATH = "/Users/rdahis/acs_data/output/microdata_person"
EXPECT = 308_855_872

_orig = gcs.Client.bucket
gcs.Client.bucket = lambda self, name, user_project=None: _orig(
    self, name, user_project=BILLING
)

st = bd.Storage(dataset_id=DATASET, table_id=TABLE)
ok = False
for a in range(1, 21):
    try:
        print(
            f"[storage upload attempt {a}] {time.strftime('%H:%M:%S')}",
            flush=True,
        )
        st.upload(
            path=PATH,
            mode="staging",
            if_exists="pass",
            chunk_size=50 * 1024 * 1024,
        )
        ok = True
        print("storage upload complete", flush=True)
        break
    except Exception as e:
        print(
            f"  attempt {a} failed: {type(e).__name__}: {str(e)[:140]}",
            flush=True,
        )
        time.sleep(15)
if not ok:
    raise SystemExit("storage upload failed after 20 attempts")

tb = bd.Table(dataset_id=DATASET, table_id=TABLE)
tb.create(
    path=PATH,
    source_format="parquet",
    if_table_exists="replace",
    if_storage_data_exists="pass",
    if_dataset_exists="pass",
)
print("table created", flush=True)

c = bigquery.Client(project=BILLING)
n = next(
    iter(
        c.query(
            f"select count(*) n from `{BILLING}.{DATASET}_staging.{TABLE}`"
        ).result()
    )
).n
print(
    f"ROWS={n:,} ({'MATCH' if n == EXPECT else f'MISMATCH expected {EXPECT:,}'})",
    flush=True,
)
