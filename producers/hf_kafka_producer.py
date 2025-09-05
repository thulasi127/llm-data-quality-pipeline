"""
Hugging Face → Kafka producer (streams real text into 'raw_text' topic)

Usage:
  HF_DATASET=openwebtext HF_SPLIT=train python producers/hf_kafka_producer.py
  # or specify another dataset/split:
  HF_DATASET=wikipedia HF_CONFIG=20220301.en HF_SPLIT=train

Env vars:
  HF_DATASET  (default: openwebtext)
  HF_CONFIG   (optional, e.g., wikipedia config language like 20220301.en)
  HF_SPLIT    (default: train)
  LIMIT       (optional int; stop after N records)
  THROTTLE_MS (optional int; sleep per message, default 2ms)

Notes:
  - You'll need: pip install datasets
  - Some datasets require 'config' and/or auth. See: https://huggingface.co/datasets
"""
import os, uuid, json, time, datetime as dt, random
from datasets import load_dataset, DownloadConfig
from confluent_kafka import Producer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP","localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC","raw_text")

HF_DATASET = os.environ.get("HF_DATASET","openwebtext")
HF_CONFIG  = os.environ.get("HF_CONFIG", None)
HF_SPLIT   = os.environ.get("HF_SPLIT","train")
LIMIT      = int(os.environ.get("LIMIT","2000"))
THROTTLE_MS = int(os.environ.get("THROTTLE_MS","2"))

domains = ["news","code","social","docs"]
categories = ["ai","finance","health","education"]

def record_from_text(text: str):
    return {
        "id": str(uuid.uuid4()),
        "ts": dt.datetime.utcnow().isoformat(),
        "text": text,
        "source": "web",
        "domain": random.choice(domains),
        "category": random.choice(categories)
    }

def iter_text(example):
    # Try to extract a text field from common datasets
    for key in ("text", "content", "article", "body"):
        if key in example and isinstance(example[key], str):
            return example[key]
    # Fallback: join string-like fields
    parts = []
    for k,v in example.items():
        if isinstance(v,str) and len(v) > 0:
            parts.append(v)
    return " ".join(parts) if parts else None

def main():
    print(f"Loading dataset: {HF_DATASET} (config={HF_CONFIG}) split={HF_SPLIT}")
    download_config = DownloadConfig(local_files_only=False)
    ds = load_dataset(HF_DATASET, HF_CONFIG, split=HF_SPLIT, streaming=True, download_config=download_config)

    p = Producer({'bootstrap.servers': BOOTSTRAP})
    sent = 0
    for ex in ds:
        text = iter_text(ex)
        if not text:
            continue
        rec = record_from_text(text)
        p.produce(TOPIC, json.dumps(rec).encode())
        sent += 1
        if sent % 500 == 0:
            p.flush()
            print(f"Sent {sent} messages → topic '{TOPIC}'")
        if THROTTLE_MS:
            time.sleep(THROTTLE_MS/1000.0)
        if LIMIT and sent >= LIMIT:
            break

    p.flush()
    print(f"Done. Sent {sent} messages from '{HF_DATASET}' split '{HF_SPLIT}' to topic '{TOPIC}'.")

if __name__ == "__main__":
    main()
