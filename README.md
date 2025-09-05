# LLM Dataset Quality Pipeline (MVP)

An end-to-end, production-styled path from **raw text** to **curated, versioned datasets** with **live quality telemetry**. Built to demonstrate how we can keep “garbage out” before it becomes “garbage in” for ML/LLM training.

---

## 1) Problem (what breaks and why)
Web-scale text is messy: duplicates, off-language content, profanity, and fragments that don’t say anything. If we don’t gate this early, models learn the wrong lessons. The goal here is simple and measurable: **ingest → validate → store clean data with provenance → surface quality signals**.

---

## 2) Scope
### In scope (MVP)
- **Streaming ingest** via Kafka (Redpanda)
- **Deterministic quality gates**: length, language≈English, profanity list, within-batch de-dup
- **Medallion storage** in Parquet: **Bronze** (raw), **Silver** (clean), **Gold** (curated/day-partitioned)
- **Run manifest (JSON)**: counts, rejection breakdown, artifact paths
- **Dashboard** (Streamlit + DuckDB): KPIs, reasons, trends, samples
- **Orchestration** with Prefect (single node)

### Out of scope
- Model training/fine-tuning
- Petabyte-scale Spark/Airflow clusters
- Ent-grade auth/RBAC, multi-tenant controls
- Full PII/advanced toxicity detection

---

## 3) High-level design
```
Data Source → Kafka (Redpanda) → Prefect Flow
           → Validation (len/lang/profanity/dupe)
           → Bronze (raw) + Silver (clean)
           → Gold (curated, partitioned by day)
           → Streamlit (KPIs, reasons, trends, samples)
```
Design notes:
- Push work to the edges: validate before writing “good” data.
- Keep **Bronze** immutable for audit/replay. Treat **Silver/Gold** as contracts.
- Make quality visible: trend lines and reason codes or it didn’t happen.

---

## 4) Low-level design
### Data model (post-cleaning)
- `id` (UUID), `ts` (ISO-8601)
- `text` (string), `text_len` (int)
- `source` (`web|doc|code`), `domain` (`news|code|social|docs`), `category` (`ai|finance|health|education`)

### Quality gates (MVP)
- **Length:** 20 ≤ `text_len` ≤ 4000
- **Language:** heuristic English check
- **Profanity:** rule-based deny list
- **Duplicates:** drop within batch on `(source, text)`

### Storage layout
```
data/
  bronze/run_ts=YYYYMMDDTHHMMSS/raw.parquet
  silver/run_ts=YYYYMMDDTHHMMSS/good.parquet
  rejects/run_ts=YYYYMMDDTHHMMSS/bad.parquet
  gold/ds=YYYY-MM-DD/gold.parquet
manifests/versions.jsonl   # one JSON line per run (counts, reasons, paths)
```

### Components
- **Kafka/Redpanda:** durable ingest, local dev via docker-compose
- **Prefect flow:** batch on N≈1000 or t≈30s; write Bronze/Silver/Rejects/Gold + manifest
- **DuckDB:** fast Parquet scans for the UI
- **Streamlit:** KPIs, rejection mix, trend, and samples

---

## 5) Success criteria
- Rising **Pass %** over time as gates and sources improve
- Rejection reasons explain most failures (no “unknown” buckets)
- Gold is stable (schema) and complete (daily partition present)
- Manifest provides reproducible lineage for any run

---

## 6) How to run system
### Prereqs
- **Docker Desktop** (for Redpanda/Kafka)
- **Python 3.9+**

### Quickstart (synthetic source)
```bash
# 0) Clone & enter
cd llm-data-quality-pipeline

# 1) Start Kafka API (Redpanda)
docker compose up -d

# 2) Python env
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

# 3) Produce synthetic messages → Kafka: raw_text
python producers/kafka_text_producer.py

# 4) Run a pipeline batch (ingest→validate→persist→manifest)
python prefect_flows/pipeline.py

# 5) Launch the dashboard
python -m streamlit run streamlit_app/app.py
# open http://localhost:8501
```

**Dashboard shows**
- KPIs: Ingested / Passed / Rejected / Pass %
- Rejection reasons (length / language / profanity / duplicate)
- Domain/Category mix
- Pass % trend across runs
- Samples: Gold and Rejects

### Real source (Hugging Face → Kafka)
```bash
python -m pip install datasets

# Example (v3-compatible): English Wikipedia
HF_DATASET=wikipedia HF_CONFIG=20220301.en HF_SPLIT=train LIMIT=2000   python producers/hf_kafka_producer.py

# Another example
HF_DATASET=ag_news HF_SPLIT=train LIMIT=2000   python producers/hf_kafka_producer.py

# Then run pipeline + UI
python prefect_flows/pipeline.py
python -m streamlit run streamlit_app/app.py
```

**Producer env vars**
`HF_DATASET`, `HF_CONFIG` (if required), `HF_SPLIT`, `LIMIT`, `THROTTLE_MS`, `KAFKA_BOOTSTRAP` (default `localhost:9092`), `KAFKA_TOPIC` (default `raw_text`).  
> On `datasets` v3: some script-based datasets (e.g., `openwebtext`) are removed. Use v3-compatible sets (e.g., `wikipedia`, `ag_news`, `imdb`) or pin `datasets<3.0` if needed.

---

## 7) Troubleshooting
- **`docker: command not found`** → start Docker Desktop; on macOS:  
  `export PATH=$PATH:/Applications/Docker.app/Contents/Resources/bin`
- **Broker unavailable** → give Redpanda a few seconds; `docker compose logs -f`
- **Moved folder → “bad interpreter”** → rebuild venv:
  ```bash
  deactivate 2>/dev/null || true
  rm -rf .venv
  python3 -m venv .venv
  source .venv/bin/activate
  python -m pip install -r requirements.txt
  ```
- **HF error “Dataset scripts no longer supported”** → use v3-compatible datasets or `python -m pip install "datasets<3.0"`
- **Port 9092 busy** → stop other Kafka or change port in `docker-compose.yml` and set `KAFKA_BOOTSTRAP` accordingly

---

## 8) Notes & next steps
- Cross-run dedupe (content hashes) for Gold
- PII/advanced toxicity checks (regex/ML)
- Export “latest Gold” from UI; add MLflow lineage
- Airflow/Spark variants for scale-out

---

