Here’s a **lean MVP stack** that gets “paste Iceberg creds → run SQL → see rows <60s” without yak-shaving.

### Product slice

* **Frontend**: Plain HTML, CSS, JS for speed and no fighting with NPM
* **Auth**: Sign in with Google — frictionless, no passwords.
* **Backend API**: FastAPI (Python 3.11) with three endpoints:

  * `POST /connect/test` – validate S3/R2 access to a given Iceberg table path.
  * `POST /query` – run SQL with time/byte limits; stream first N rows.
  * `GET /tables` – optional: list tables from a given prefix.
* **Query engine**: DuckDB **in-process** (not per-query containers) with extensions:

  * `httpfs` (S3/R2), `iceberg` (Iceberg tables).
* **State**: PostgreSQL (RDS or Supabase) for users, saved connections (encrypted), saved queries, and audit logs.
* **Infra**: Single ECS Fargate service (FastAPI), behind an ALB. CloudWatch Logs. Parameter Store for config.

### Why this is minimal (and fast)

* No Trino/Athena. No job queue. No per-query container spin-up. DuckDB runs inside FastAPI with **strict guardrails**.
* Works for both **AWS S3** and **Cloudflare R2** (S3-compatible endpoint).
* Easy to evolve later into “heavy mode” (background workers / per-query containers) once usage grows.

---

### Guardrails (the secret to a safe MVP)

* **Row cap**: default 1,000 rows returned.
* **Scan cap**: set `s3_max_single_part_download_size` and track `read_bytes` via DuckDB profiling; kill if > e.g. 512 MB.
* **Timeout**: cancel query after e.g. 15s.
* **Read-only**: deny `COPY/INSERT/UPDATE/DELETE/PRAGMA` except allowed `INSTALL/LOAD` extensions at startup.
* **Ephemeral creds**: require **temporary STS** (AWS) or R2 **token**; never store long-lived keys. Encrypt at rest.

---

### Connection model (MVP)

User pastes:

* **Catalog type**: “file layout” Iceberg (point at table root), or “REST/Hive” (optional later).
* **Object store**: `s3://bucket/path` **or** R2 endpoint + bucket/path.
* **Temp credentials**: Access key/secret/session token (AWS STS) or R2 access key/secret.

Backend maps to DuckDB:

```sql
INSTALL httpfs; LOAD httpfs;
INSTALL iceberg; LOAD iceberg;

-- S3 (AWS)
SET s3_region='eu-west-1';
SET s3_access_key_id='…';
SET s3_secret_access_key='…';
SET s3_session_token='…';
-- R2 (Cloudflare)
SET s3_endpoint='https://<accountid>.r2.cloudflarestorage.com';
SET s3_url_style='path';
SET s3_use_ssl=true;

-- Query an Iceberg table
SELECT * FROM iceberg_scan('s3://my-bucket/db/table') LIMIT 1000;
```

> Note: AWS users can also supply an **AssumeRole ARN**; the backend calls STS to mint temp creds server-side.

---

### FastAPI “run query” (core path)

* Start DuckDB once per worker with extensions preloaded.
* For each request: `BEGIN;` apply per-request S3 settings; run SQL with timeout; stream Arrow/Parquet back.

Pseudocode sketch (trimmed):

```python
import duckdb, time
from fastapi import FastAPI, HTTPException

db = duckdb.connect(":memory:")
db.execute("INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;")

def apply_s3(creds):
    for k,v in creds.items():
        db.execute(f"SET {k}='{v}';")

app = FastAPI()

@app.post("/query")
def run_query(sql: str, s3: dict):
    apply_s3(s3)
    db.execute("PRAGMA threads=4; PRAGMA enable_profiling=json;")
    t0 = time.time()
    try:
        res = db.execute(sql + " LIMIT 1000").fetch_arrow_table()
    except Exception as e:
        raise HTTPException(400, str(e))
    if time.time() - t0 > 15:  # hard timeout
        raise HTTPException(408, "Query timeout")
    return arrow_table_to_json(res)  # or stream Arrow/CSV
```

---

### Deploy (AWS-first)

* **ECR**: push FastAPI image (uvicorn + duckdb + extensions).
* **ECS Fargate**: 1 service, 1–2 tasks, auto-scale on CPU/requests.
* **ALB**: HTTPS via ACM.
* **RDS Postgres**: t4g.small.
* **Secrets**: AWS SSM Parameter Store + KMS.
* **Terraform**: VPC, subnets, ALB, ECS service, task role with **no S3 data perms** (users supply creds), CloudWatch logs.

---

### Frontend UX (one screen, one minute)

* Left panel: **Connection** (S3/R2 endpoint, bucket, path, temp creds).
* Center: **SQL editor** , **Run** button.
* Right: **Results** grid + “bytes scanned / time / approx cost”.
* Footer: **Save query** / **Share link**.

---

### Stretch (still small)

* **Saved connections**: KMS-encrypted, opt-in (else ephemeral).
* **Cost hint**: `bytes_scanned * storage_egress_price` rough estimate per cloud.

---

### Why this wins the wedge

* From zero to first rows with **no cluster** and **no vendor lock-in**.
* Clear constraints to avoid blast radius.
* Clean path to “heavy mode” later (per-query containers, caching, BI connectors).
