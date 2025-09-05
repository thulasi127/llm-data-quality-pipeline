# LLM Dataset Quality Pipeline

Streaming **text ingestion → automated data-quality gates → Medallion storage (Bronze/Silver/Gold) → live dashboard**.  
Great for ML/LLM data engineering interviews and real-world prototyping.

## Why this exists
LLMs are only as good as their data. Web-scale text is noisy: **duplicates**, **non-English**, **profanity**, and **too short/long** content. This pipeline ingests streaming text, validates/cleans it, versions curated datasets, and surfaces **clear quality metrics** in a small UI.

---

## Features
- **Kafka (Redpanda)** streaming ingest
- **Validation gates:** length (20–4000), language≈English, profanity filter, within-batch dedupe
- **Medallion storage:** **Bronze** (raw), **Silver** (clean), **Gold** (curated)
- **Run manifest (JSONL):** counts, rejection reasons, artifact paths
- **Streamlit dashboard:** KPIs, rejection breakdown, domain/category mix, trend line, gold/reject samples
- **Prefect** (single-node) orchestration
- **DuckDB** queries over Parquet
- **Optional:** Hugging Face → Kafka producer for real datasets

---

## Architecture

```
Data Source → Kafka (Redpanda) → Prefect Flow
           → Validation (len/lang/profanity/dupe)
           → Bronze (raw) + Silver (clean)
           → Gold (curated, partitioned by day)
           → Streamlit Dashboard (KPIs, reasons, trends, samples)
```

**Per-run flow**
1) Producer publishes JSON to topic `raw_text`  
2) Prefect consumes a batch (e.g., ~1,000 msgs or 30s)  
3) Validate records (length, language, profanity, dedupe)  
4) Persist **Bronze** (raw), **Silver** (passed), **Rejects** (failed with reason), **Gold** (curated by date)  
5) Append **manifest** entry (counts, reasons, paths)  
6) Dashboard reads Parquet + manifest via **DuckDB**

---

## Project Structure
```
.
├─ docker-compose.yml            # Redpanda (Kafka API) local
├─ requirements.txt
├─ README.md
├─ .gitignore
├─ producers/
│  ├─ kafka_text_producer.py     # synthetic data → Kafka
│  └─ hf_kafka_producer.py       # Hugging Face dataset → Kafka
├─ prefect_flows/
│  └─ pipeline.py                # consume → bronze/silver/rejects/gold → manifest
├─ streamlit_app/
│  └─ app.py                     # dashboard
├─ ge/expectations/text_suite.json   # placeholder for future checks
├─ data/                         # (gitignored) bronze/silver/rejects/gold at runtime
└─ manifests/                    # (gitignored) versions.jsonl at runtime
```

---

## Medallion (Bronze / Silver / Gold)

- **Bronze (raw/landing)**  
  Exact batch as ingested (**pre-validation**). Immutable; used for **audit, replay, forensics**.  
  `data/bronze/run_ts=YYYYMMDDTHHMMSS/raw.parquet`

- **Silver (cleaned/conformed)**  
  Records that **pass gates** with a consistent schema; includes derived fields like `text_len`.  
  Gates: length 20–4000, language≈English, no profanity, no within-batch duplicates.  
  `data/silver/run_ts=YYYYMMDDTHHMMSS/good.parquet`  
  **Rejects** (with `failure_reason`): `data/rejects/run_ts=YYYYMMDDTHHMMSS/bad.parquet`

- **Gold (curated/consumption-ready)**  
  Training/analytics-ready dataset, **partitioned by day**. (MVP writes from Silver; easy to extend with cross-run dedupe).  
  `data/gold/ds=YYYY-MM-DD/gold.parquet`

- **Manifest**  
  One JSON line per run with counts, reasons, and artifact paths:  
  `manifests/versions.jsonl`

---

## Prerequisites
- **Docker Desktop** (to run Redpanda/Kafka)
- **Python 3.9+**
- **Git** (to clone/publish)

> **macOS tip:** if `docker` isn’t found, ensure Docker Desktop is running and add:
> ```
> export PATH=$PATH:/Applications/Docker.app/Contents/Resources/bin
> ```

---

## Quickstart (synthetic data)

```bash
# 0) Clone & enter
git clone <your-repo-url> llm-data-quality-pipeline
cd llm-data-quality-pipeline

# 1) Start Redpanda (Kafka API) locally
docker compose up -d

# 2) Python virtual env
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

# 3) Produce synthetic messages
python producers/kafka_text_producer.py

# 4) Run one pipeline batch
python prefect_flows/pipeline.py

# 5) Open the dashboard
python -m streamlit run streamlit_app/app.py
# visit http://localhost:8501
```

You’ll see:
- KPIs: **Ingested / Passed / Rejected / Pass %**
- **Rejection reasons** chart (length / language / profanity / duplicate)
- **Domain/Category** breakdown (for passed data)
- **Trend** of pass% across runs
- **Gold** and **Rejects** sample tables

---

## Real data source (Hugging Face → Kafka)

```bash
# In your venv:
python -m pip install datasets

# Start broker if not running
docker compose up -d

# Example: English Wikipedia (v3-compatible; requires config)
HF_DATASET=wikipedia HF_CONFIG=20220301.en HF_SPLIT=train LIMIT=2000   python producers/hf_kafka_producer.py

# Another v3-compatible example:
HF_DATASET=ag_news HF_SPLIT=train LIMIT=2000   python producers/hf_kafka_producer.py

# Then run pipeline + dashboard as usual:
python prefect_flows/pipeline.py
python -m streamlit run streamlit_app/app.py
```

**Producer env vars**
- `HF_DATASET` — e.g., `wikipedia`, `ag_news`, `imdb`  
- `HF_CONFIG` — required for some datasets (e.g., `20220301.en`)  
- `HF_SPLIT` — default `train`  
- `LIMIT` — max records to send (default `2000`)  
- `THROTTLE_MS` — per-message delay (default `2`)  
- `KAFKA_BOOTSTRAP` — default `localhost:9092`  
- `KAFKA_TOPIC` — default `raw_text`

> **Note:** `datasets` v3 removed certain *script-based* datasets (e.g., `openwebtext`).  
> Use a v3-compatible dataset (like `wikipedia`, `ag_news`, `imdb`) **or** pin `datasets<3.0` if you need older ones:
> ```
> python -m pip install "datasets<3.0"
> HF_DATASET=openwebtext HF_SPLIT=train LIMIT=2000 python producers/hf_kafka_producer.py
> ```

---

## Data Contract & Quality Gates

**Schema (post-cleaning):**
- `id` (string, UUID)
- `ts` (ISO-8601 UTC)
- `text` (string)
- `source` (enum: `web|doc|code`)
- `domain` (enum: `news|code|social|docs`)
- `category` (enum: `ai|finance|health|education`)
- `text_len` (int; derived)

**Validation checks:**
- Text length **20–4000**
- Language **≈ English** (heuristic)
- **No profanity**
- **No duplicates** per `(source, text)` within batch

---

## Configuration (paths & topics)
- **Kafka topic:** `raw_text` (default)
- **Outputs:**
  - Bronze: `data/bronze/run_ts=.../raw.parquet`
  - Silver: `data/silver/run_ts=.../good.parquet`
  - Rejects: `data/rejects/run_ts=.../bad.parquet`
  - Gold: `data/gold/ds=YYYY-MM-DD/gold.parquet`
  - Manifest: `manifests/versions.jsonl`

---

## Troubleshooting

**`docker: command not found`**  
- Start Docker Desktop; on macOS add:
  ```
  export PATH=$PATH:/Applications/Docker.app/Contents/Resources/bin
  ```

**Producer/pipeline “broker” errors**  
- Wait a few seconds after `docker compose up -d`  
- Check logs:
  ```
  docker compose logs -f
  ```

**“bad interpreter” in `.venv/bin/*` after moving folders**  
- Recreate venv in the current folder:
  ```
  deactivate 2>/dev/null || true
  rm -rf .venv
  python3 -m venv .venv
  source .venv/bin/activate
  python -m pip install -r requirements.txt
  ```
- Run Streamlit via module form:
  ```
  python -m streamlit run streamlit_app/app.py
  ```

**Hugging Face “Dataset scripts are no longer supported”**  
- Use v3-compatible datasets (`wikipedia`, `ag_news`, `imdb`)  
- Or pin: `python -m pip install "datasets<3.0"`

**Port 9092 in use**  
- Stop other Kafka; or change port mapping in `docker-compose.yml` and set `KAFKA_BOOTSTRAP` to match.

---

## Git Hygiene
Keep runtime data out of git:
```
# .gitignore (recommended)
.venv/
__pycache__/
*.pyc
data/
manifests/
.streamlit/
```
Optionally keep folder structure without data:
```
data/*         # ignore all
!data/.keep    # keep marker
manifests/*
!manifests/.keep
```

---

## Roadmap
- Cross-run dedupe for Gold via content hashes
- PII regex checks (emails/phones) → rejection reason & chart
- “Download Gold” button in the dashboard
- MLflow logging: dataset hash + pass% for lineage
- Airflow DAG variant (alt branch)
- Spark/Scala variant (scale-out)

---

## References
- **Redpanda (Kafka API)** — https://redpanda.com  
- **Prefect** — https://www.prefect.io  
- **Streamlit** — https://streamlit.io  
- **DuckDB** — https://duckdb.org  
- **Hugging Face Datasets** — https://huggingface.co/docs/datasets

---

_Questions or ideas? Open an issue or PR. Happy streaming!_
