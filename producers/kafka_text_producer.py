from confluent_kafka import Producer
import json, time, uuid, random, datetime as dt

p = Producer({'bootstrap.servers':'localhost:9092'})

# Richer corpus to exercise validators and UI facets
clean_english = [
  "Large language models rely on curated datasets with strong data quality checks to prevent bias and toxicity.",
  "Data engineers design pipelines that ingest, validate, and version data so model training remains reproducible.",
  "Streaming architectures with Kafka enable near real-time validation and feedback loops for model data curation.",
  "Delta-style datasets help track versions and lineage, supporting audits and safe rollbacks in ML systems.",
  "Observability dashboards surface pass rates and failure reasons, guiding on-call response and root-cause analysis."
]

short_texts = ["short", "tiny", "ok", "bad", "no"]
non_english = [
  "Este texto está en español y debe ser rechazado por idioma.",  # es
  "Ceci est un texte en français qui sera rejeté.",                # fr
  "这是一段中文文本，应该因为语言而被拒绝。",                            # zh
]
profanity_texts = [
  "This line includes $#@! profanity and should be rejected.",
  "Inappropriate words appear here which must not pass validation."
]
duplicates = [
  "This sentence is intentionally duplicated to test de-duplication in the batch.",
  "This sentence is intentionally duplicated to test de-duplication in the batch."
]

domains = ["news","code","social","docs"]
categories = ["ai","finance","health","education"]

def delivery(err, msg): pass

def make_record(text):
    return {
      "id": str(uuid.uuid4()),
      "ts": dt.datetime.utcnow().isoformat(),
      "text": text,
      "source": random.choice(["web","doc","code"]),
      "domain": random.choice(domains),
      "category": random.choice(categories)
    }

pool = (
    clean_english * 50 +
    short_texts * 40 +
    non_english * 40 +
    profanity_texts * 40 +
    duplicates * 60
)

random.shuffle(pool)

count = 1200
for i in range(count):
    rec = make_record(pool[i % len(pool)])
    p.produce("raw_text", json.dumps(rec).encode(), callback=delivery)
    if i % 100 == 0:
        p.flush()
    time.sleep(0.003)

p.flush()
print(f"Produced {count} messages to topic 'raw_text'")
