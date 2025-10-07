# 🌊 Cloudfloe

**Query your Apache Iceberg data lake in seconds. No clusters. No ops. Just SQL.**

![Cloudfloe Screenshot](images/cloudfloe_scrrenshot.png)

## ⚡ The Problem

You have data in Apache Iceberg. You just want to **query it correctly**.

But here's what you face:

- **Trino/Presto**: Heavy clusters, complex setup, operational overhead
- **AWS Athena**: Vendor lock-in, slower iteration, costs add up
- **Local DuckDB**: Works great solo, painful to share and collaborate
- **Spark**: Overkill for exploratory queries, slow startup
- **Direct Parquet reads**: Fast but **dangerous** — bypasses Iceberg metadata, can return deleted rows

**You don't need a hammer when you need a magnifying glass.**

---

## 💡 The Solution: Cloudfloe

Cloudfloe is **DuckDB-as-a-service** for Apache Iceberg data lakes.

### What it does:
- 🧊 **Reads Iceberg correctly** — uses metadata layer, validates snapshots
- 🚀 **Instant queries** on S3, R2, or MinIO — no data movement
- 🌐 **Browser-based SQL editor** — no CLI, no local setup
- 🔓 **Zero lock-in** — you own the data, we just query it
- ⚡ **Sub-second startup** — no cluster spin-up time

### What it doesn't do:
- ❌ Store your data (you keep it where it is)
- ❌ Require infrastructure changes (just S3 credentials)
- ❌ Support write operations (read-only by design)
- ❌ Handle tables with row-level deletes (append-only Iceberg v1/v2 only)

**Think of it as**: A collaborative, web-based scratchpad for your Iceberg data lake.

---

## ✨ Current Features (Phase 1)

| Feature | Description |
|---------|-------------|
| 🧊 **Iceberg Native** | Reads via `iceberg_scan()` — respects Iceberg metadata and snapshots |
| ✅ **Table Validation** | Auto-detects row-level deletes and rejects unsafe tables |
| 🔌 **Multi-Cloud Support** | AWS S3, Cloudflare R2, MinIO — any S3-compatible storage |
| 🖥️ **Web SQL Editor** | Syntax highlighting, query history, sample queries |
| 📊 **Instant Results** | DuckDB 1.4.1 engine, no cluster warmup, sub-second queries |
| 🔒 **Read-Only by Design** | No destructive operations — query, don't mutate |
| 🐳 **Docker Ready** | One command to run locally, no complex setup |

---

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- S3-compatible storage with Iceberg table (or use our demo data)

### 1. Start Cloudfloe
```bash
git clone https://github.com/gordonmurray/cloudfloe
cd cloudfloe
docker compose up --build
```

Wait ~30 seconds for initialization, then open **http://localhost:3000**

### 2. Connect to Your Iceberg Table

In the UI Connection panel, enter:

**AWS S3 Example:**
```
Storage Type: AWS S3
AWS S3 Endpoint: s3.amazonaws.com (or leave blank for default)
Table Path: s3://your-bucket/warehouse/db/table_name
Access Key: AKIAIOSFODNN7EXAMPLE
Secret Key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
Region: us-east-1
```

**Important Notes:**
- **Table Path** should point to the Iceberg table root (where `/metadata` folder is located)
- Do NOT include `/metadata` in the path — Cloudfloe adds it automatically
- Trailing slashes are automatically removed

Click **"Test Connection"** — if successful, a sample query will appear in the editor.

### 3. Run Your First Query

After connection succeeds, a query like this will be auto-loaded:

```sql
SELECT * FROM iceberg_scan('s3://your-bucket/warehouse/db/table_name') LIMIT 10;
```

Just click **"Run Query"** to see your data!

---

## 📖 Querying Iceberg Tables

### Basic Query
```sql
SELECT * FROM iceberg_scan('s3://bucket/warehouse/db/table_name') LIMIT 100;
```

### With Filters
```sql
SELECT user_id, event_type, timestamp
FROM iceberg_scan('s3://bucket/warehouse/events/user_events')
WHERE event_type = 'purchase'
  AND timestamp > '2024-01-01'
ORDER BY timestamp DESC;
```

### Aggregations
```sql
SELECT
    date_trunc('day', timestamp) as day,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users
FROM iceberg_scan('s3://bucket/warehouse/events/user_events')
GROUP BY day
ORDER BY day DESC;
```

### Inspect Iceberg Metadata
```sql
-- View table snapshots
SELECT * FROM iceberg_snapshots('s3://bucket/warehouse/db/table_name');

-- View table metadata (manifests, partitions, etc)
SELECT * FROM iceberg_metadata('s3://bucket/warehouse/db/table_name');
```

---

## 🔐 Setting Up S3 Access

### IAM Policy for AWS S3

Your AWS credentials need these permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-bucket-name",
        "arn:aws:s3:::your-bucket-name/*"
      ]
    }
  ]
}
```

**Minimum required:**
- `s3:ListBucket` — To list files in `/metadata` directory
- `s3:GetObject` — To read metadata and data files

### Testing Access

Before using Cloudfloe, verify access with AWS CLI:

```bash
# Test 1: Can you list the metadata directory?
aws s3 ls s3://your-bucket/warehouse/db/table_name/metadata/

# Test 2: Can you read the version hint?
aws s3 cp s3://your-bucket/warehouse/db/table_name/metadata/version-hint.text -
```

If these work, Cloudfloe will work too.

---

## 🛡️ Important Limitations

### ✅ Supported:
- Iceberg v1 and v2 table formats
- Append-only tables (no deletes)
- Parquet data files
- Time travel queries (via snapshots)
- Partition pruning (DuckDB handles this)

### ❌ Not Yet Supported:
- **Row-level deletes** — Tables with position or equality deletes will be rejected
- **Write operations** — Read-only for now (writes planned for Phase 2)
- **REST Catalog** — Direct S3 path access only
- **Schema evolution** — Reads current schema, doesn't handle complex migrations

**If your table has deletes**, you'll see:
```
Error: Table contains row-level deletes which are not supported.
This application only supports append-only Iceberg v1/v2 tables.
```

**Solution:** Compact your table first:
```sql
-- In Spark/Trino/Iceberg CLI:
CALL compact_table('your_table');
```

---

## 🏗️ Architecture

```
┌─────────────────┐
│   Frontend      │  ← Nginx + HTML/CSS/JS
│  (Port 3000)    │     CodeMirror SQL Editor
└────────┬────────┘
         │
         ↓ HTTP
┌─────────────────┐
│   Backend       │  ← FastAPI + Python
│  (Port 8000)    │     DuckDB 1.4.1 + Iceberg Extension
└────────┬────────┘
         │
         ↓ S3 API
┌─────────────────┐
│   S3 Storage    │  ← AWS S3 / R2 / MinIO
│                 │     Iceberg table (metadata + data)
└─────────────────┘
```

**Key Components:**
- **DuckDB 1.4.1**: Query engine with native Iceberg support
- **Iceberg Extension**: Reads Iceberg metadata and manifests
- **FastAPI**: REST API for query execution and connection testing
- **HTTPFS Extension**: S3-compatible storage access

---

## 🧪 Local Development

### Run with Docker Compose (Recommended)
```bash
docker compose up --build
```

### Run Backend Manually
```bash
cd backend
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload
```

Backend runs on http://localhost:8000

### Run Frontend Manually
```bash
cd frontend
python3 -m http.server 3000
```

Frontend runs on http://localhost:3000

---

## 📊 Query Stats

After running a query, click the **"Query Stats"** tab to see:

- **Execution Time**: How long the query took (milliseconds)
- **Bytes Scanned**: Approximate data size processed
- **Rows Returned**: Number of rows in the result set

Note: Bytes scanned is a rough estimate based on returned data, not actual S3 bytes read.

---

## 🤝 Contributing

Cloudfloe is in active development. Contributions welcome!

## 💬 Questions?

Open an issue on GitHub for bugs or feature requests
