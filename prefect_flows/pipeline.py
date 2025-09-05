# prefect_flows/pipeline.py
import json, os, pathlib, time, datetime as dt
import pandas as pd
from prefect import flow, task
from confluent_kafka import Consumer
from langdetect import detect
from better_profanity import profanity
import duckdb

profanity.load_censor_words()

def kafka_consumer():
    c = Consumer({
        'bootstrap.servers':'localhost:9092',
        'group.id':'dq',
        'auto.offset.reset':'earliest'
    })
    c.subscribe(['raw_text'])
    return c

@task
def consume_batch(max_msgs=1500, timeout_s=30):
    c = kafka_consumer()
    rows = []
    start = time.time()
    while len(rows) < max_msgs and (time.time()-start) < timeout_s:
        msg = c.poll(0.2)
        if msg is None or msg.error():
            continue
        rows.append(json.loads(msg.value().decode()))
    c.close()
    if not rows:
        return pd.DataFrame(columns=["id","ts","text","source","domain","category"])
    df = pd.DataFrame(rows)
    for col in ["id","ts","text","source","domain","category"]:
        if col not in df.columns:
            df[col] = None
    return df

@task
def write_bronze(df: pd.DataFrame, run_ts: str):
    """Persist the exact batch as ingested (pre-validation)."""
    base = pathlib.Path(__file__).resolve().parents[1] / "data"
    bronze = base / "bronze" / f"run_ts={run_ts}"
    bronze.mkdir(parents=True, exist_ok=True)
    if df.empty:
        (bronze/"EMPTY").touch()
    else:
        df.to_parquet(bronze/"raw.parquet", index=False)
    return str(bronze)

def _is_en(s: str) -> bool:
    try:
        return detect(s) == "en"
    except:
        return False

def validate_df(df: pd.DataFrame):
    if df.empty:
        df["failure_reason"] = []
        return df, df
    df = df.assign(text=df["text"].astype(str).str.strip())
    df["text_len"] = df["text"].str.len()

    len_ok = df["text_len"].between(20, 4000)
    lang_ok = df["text"].apply(_is_en)
    prof = df["text"].apply(lambda t: profanity.contains_profanity(t))
    dup_ok = ~df.duplicated(subset=["source","text"], keep='first')

    ok = len_ok & lang_ok & ~prof & dup_ok
    passed = df[ok].copy()
    failed = df[~ok].copy()
    reasons = []
    for i, _ in failed.iterrows():
        r = []
        if not len_ok.loc[i]: r.append("length")
        if not lang_ok.loc[i]: r.append("language")
        if prof.loc[i]: r.append("profanity")
        if not dup_ok.loc[i]: r.append("duplicate")
        reasons.append(",".join(r) if r else "unknown")
    failed["failure_reason"] = reasons
    keep_cols = ["id","ts","text","source","domain","category","text_len"]
    return passed[keep_cols], failed[keep_cols+["failure_reason"]]

@task
def write_outputs(passed: pd.DataFrame, failed: pd.DataFrame, run_ts: str):
    base = pathlib.Path(__file__).resolve().parents[1] / "data"
    silver = base / "silver" / f"run_ts={run_ts}"
    rejects = base / "rejects" / f"run_ts={run_ts}"
    silver.mkdir(parents=True, exist_ok=True)
    rejects.mkdir(parents=True, exist_ok=True)

    if not passed.empty:
        passed.to_parquet(silver/"good.parquet", index=False)
    else:
        (silver/"EMPTY").touch()

    if not failed.empty:
        failed.to_parquet(rejects/"bad.parquet", index=False)
    else:
        (rejects/"EMPTY").touch()

    # Curate gold for today's date
    ds = dt.datetime.utcnow().strftime("%Y-%m-%d")
    gold_dir = base / "gold" / f"ds={ds}"
    gold_dir.mkdir(parents=True, exist_ok=True)

    # (MVP) write today's gold from 'good'; you can add cross-run dedupe later
    con = duckdb.connect(database=":memory:")
    if (silver/"good.parquet").exists():
        con.execute("CREATE TABLE good AS SELECT * FROM read_parquet(?)", [str(silver/"good.parquet")])
        con.execute("CREATE TABLE gold AS SELECT * FROM good")
        con.execute(f"COPY gold TO '{gold_dir}/gold.parquet' (FORMAT PARQUET)")

    return {
        "run_ts": run_ts,
        "silver_path": str(silver),
        "rejects_path": str(rejects),
        "gold_path": str(gold_dir),
        "passed": int(len(passed)),
        "rejected": int(len(failed))
    }

@task
def append_manifest(info: dict, failed: pd.DataFrame, bronze_path: str):
    reasons = failed["failure_reason"].value_counts().to_dict() if not failed.empty else {}
    entry = {
        "run_ts": info["run_ts"],
        "in": info["passed"] + info["rejected"],
        "passed": info["passed"],
        "rejected": info["rejected"],
        "reasons": reasons,
        "bronze_path": bronze_path,
        "silver_path": info["silver_path"],
        "rejects_path": info["rejects_path"],
        "gold_path": info["gold_path"]
    }
    manifests = pathlib.Path(__file__).resolve().parents[1] / "manifests"
    manifests.mkdir(parents=True, exist_ok=True)
    with open(manifests/"versions.jsonl","a") as f:
        f.write(json.dumps(entry)+"\n")
    return entry

@flow
def dq_run():
    # one consistent run id for all artifacts
    run_ts = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    df = consume_batch()
    bronze_path = write_bronze(df, run_ts)
    passed, failed = validate_df(df)
    info = write_outputs(passed, failed, run_ts)
    entry = append_manifest(info, failed, bronze_path)
    print("Run manifest:", json.dumps(entry, indent=2))

if __name__ == "__main__":
    dq_run()
