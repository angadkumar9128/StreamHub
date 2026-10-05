"""StreamHub V1: control plane + producer API on top of Redpanda/Kafka.
Delivery is at-least-once; every event carries an event_id for downstream dedup."""
import collections, csv, hashlib, io, json, os, secrets, time, uuid
from datetime import datetime, timezone

import psycopg2, psycopg2.extras
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from fastapi import Depends, FastAPI, File, Header, HTTPException, Request, UploadFile
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

BROKERS = os.environ["KAFKA_BROKERS"]
DB_URL = os.environ["DATABASE_URL"]
ADMIN_KEY = os.environ["ADMIN_API_KEY"]
RATE_LIMIT = int(os.environ.get("RATE_LIMIT_PER_SEC", "200"))

app = FastAPI(title="StreamHub", version="0.1.0")
producer = Producer({"bootstrap.servers": BROKERS, "compression.type": "lz4",
                     "linger.ms": 20, "enable.idempotence": True})
admin = AdminClient({"bootstrap.servers": BROKERS})


# ---------- infra helpers ----------
def db():
    return psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)


@app.on_event("startup")
def init_db():
    for _ in range(30):  # wait for postgres
        try:
            with db() as c, c.cursor() as cur:
                cur.execute("""CREATE TABLE IF NOT EXISTS streams(
                    name TEXT PRIMARY KEY, partitions INT NOT NULL,
                    retention_hours INT NOT NULL, partition_key TEXT,
                    created_at TIMESTAMPTZ DEFAULT now())""")
                cur.execute("""CREATE TABLE IF NOT EXISTS schemas(stream TEXT, version INT,
                    schema JSONB NOT NULL, created_at TIMESTAMPTZ DEFAULT now(), PRIMARY KEY(stream, version))""")
                cur.execute("""CREATE TABLE IF NOT EXISTS api_keys(id SERIAL PRIMARY KEY, name TEXT,
                    role TEXT NOT NULL, key_hash TEXT UNIQUE NOT NULL, created_at TIMESTAMPTZ DEFAULT now())""")
            return
        except psycopg2.OperationalError:
            time.sleep(1)
    raise RuntimeError("postgres unavailable")


_hits = collections.defaultdict(list)


def auth(request: Request, x_api_key: str = Header(...)) -> str:
    """Roles: admin (everything), producer (publish/upload only), consumer (GET only). Per-key rate limit."""
    role = "admin" if x_api_key == ADMIN_KEY else None
    if not role:
        with db() as c, c.cursor() as cur:
            cur.execute("SELECT role FROM api_keys WHERE key_hash=%s", (hashlib.sha256(x_api_key.encode()).hexdigest(),))
            r = cur.fetchone()
        role = r["role"] if r else None
    if not role:
        raise HTTPException(401, "invalid API key")
    now = time.time()
    hits = [t for t in _hits[x_api_key] if now - t < 1]
    if len(hits) >= RATE_LIMIT:
        raise HTTPException(429, "rate limit exceeded")
    _hits[x_api_key] = hits + [now]
    p, m = request.url.path, request.method
    if role == "producer" and not (m == "POST" and p.endswith(("/events", "/events/batch", "/upload"))):
        raise HTTPException(403, "producer keys can only publish")
    if role == "consumer" and m != "GET":
        raise HTTPException(403, "consumer keys are read-only")
    return role


def admin_only(role: str = Depends(auth)):
    if role != "admin":
        raise HTTPException(403, "admin only")


TYPES = {"string": str, "integer": int, "double": (int, float), "boolean": bool}


def validate(schema: dict, payload: dict) -> dict:
    out = dict(payload)
    for f, t in schema.items():
        if f not in out:
            raise ValueError(f"missing field '{f}'")
        v = out[f]
        try:  # coerce strings (CSV) into declared types
            if isinstance(v, str) and t == "integer": v = int(v)
            elif isinstance(v, str) and t == "double": v = float(v)
            elif isinstance(v, str) and t == "boolean" and v.lower() in ("true", "false"): v = v.lower() == "true"
        except ValueError:
            pass
        if (isinstance(v, bool) and t != "boolean") or not isinstance(v, TYPES[t]):
            raise ValueError(f"field '{f}' must be {t}")
        out[f] = v
    return out


def get_stream(name: str) -> dict:
    with db() as c, c.cursor() as cur:
        cur.execute("SELECT s.*, (SELECT schema FROM schemas WHERE stream=s.name ORDER BY version DESC LIMIT 1) AS schema FROM streams s WHERE name=%s", (name,))
        row = cur.fetchone()
    if not row:
        raise HTTPException(404, f"stream '{name}' not found")
    return row


def envelope(stream: dict, payload: dict) -> tuple[str | None, bytes]:
    if stream.get("schema"):
        payload = validate(stream["schema"], payload)
    key = str(payload[stream["partition_key"]]) if stream["partition_key"] and stream["partition_key"] in payload else None
    evt = {"event_id": str(uuid.uuid4()), "stream": stream["name"], "partition_key": key,
           "event_time": datetime.now(timezone.utc).isoformat(), "payload": payload}
    return key, json.dumps(evt, default=str).encode()


def send(topic: str, key: str | None, value: bytes):
    while True:
        try:
            producer.produce(topic, value=value, key=key)
            producer.poll(0)
            return
        except BufferError:
            producer.poll(0.5)


# ---------- models ----------
class StreamIn(BaseModel):
    name: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{1,48}$")
    partitions: int = Field(3, ge=1, le=64)
    retention_hours: int = Field(24, ge=1, le=24 * 30)
    partition_key: str | None = None  # payload field used for hash partitioning


# ---------- streams ----------
@app.post("/v1/streams", dependencies=[Depends(auth)], status_code=201)
def create_stream(s: StreamIn):
    retention = {"retention.ms": str(s.retention_hours * 3600 * 1000)}
    topics = [NewTopic(s.name, s.partitions, 1, config=retention),
              NewTopic(f"{s.name}-dlq", 1, 1, config=retention)]
    for t, f in admin.create_topics(topics).items():
        try:
            f.result()
        except Exception as e:
            raise HTTPException(409, f"kafka: {e}")
    with db() as c, c.cursor() as cur:
        cur.execute("INSERT INTO streams(name,partitions,retention_hours,partition_key) VALUES(%s,%s,%s,%s)",
                    (s.name, s.partitions, s.retention_hours, s.partition_key))
    return {**s.model_dump(), "bootstrap_server_external": "localhost:19092", "topic": s.name}


@app.get("/v1/streams", dependencies=[Depends(auth)])
def list_streams():
    with db() as c, c.cursor() as cur:
        cur.execute("SELECT * FROM streams ORDER BY created_at")
        return cur.fetchall()


@app.get("/v1/streams/{name}", dependencies=[Depends(auth)])
def stream_info(name: str):
    s = get_stream(name)
    c = Consumer({"bootstrap.servers": BROKERS, "group.id": "_streamhub_meta"})
    parts = []
    for p in range(s["partitions"]):
        lo, hi = c.get_watermark_offsets(TopicPartition(name, p), timeout=5)
        parts.append({"partition": p, "earliest": lo, "latest": hi, "events": hi - lo})
    c.close()
    return {**s, "partitions_detail": parts, "total_events": sum(p["events"] for p in parts)}


# ---------- producing ----------
@app.post("/v1/streams/{name}/events", dependencies=[Depends(auth)], status_code=202)
def publish(name: str, payload: dict):
    s = get_stream(name)
    try:
        send(name, *envelope(s, payload))
    except ValueError as e:
        raise HTTPException(422, str(e))
    producer.flush(5)
    return {"accepted": 1}


@app.post("/v1/streams/{name}/events/batch", dependencies=[Depends(auth)], status_code=202)
def publish_batch(name: str, payloads: list[dict]):
    s = get_stream(name)
    for i, p in enumerate(payloads):
        try:
            send(name, *envelope(s, p))
        except ValueError as e:
            raise HTTPException(422, f"record {i}: {e}")
    producer.flush(10)
    return {"accepted": len(payloads)}


@app.post("/v1/streams/{name}/upload", dependencies=[Depends(auth)])
def upload(name: str, file: UploadFile = File(...)):
    """Streams CSV / JSONL / NDJSON line by line; the file is never fully loaded in memory.
    Bad records go to <stream>-dlq."""
    s = get_stream(name)
    fname = (file.filename or "").lower()
    text = io.TextIOWrapper(file.file, encoding="utf-8", newline="")
    ok = bad = 0
    if fname.endswith(".csv"):
        records = enumerate(csv.DictReader(text), start=2)
    elif fname.endswith((".jsonl", ".ndjson")):
        records = enumerate(text, start=1)
    elif fname.endswith(".parquet"):
        import pyarrow.parquet as pq
        records = enumerate((r for b in pq.ParquetFile(file.file).iter_batches(10000) for r in b.to_pylist()), start=1)
    else:
        raise HTTPException(415, "supported: .csv .jsonl .ndjson .parquet")
    for line_no, rec in records:
        try:
            payload = rec if isinstance(rec, dict) else json.loads(rec)
            if not isinstance(payload, dict):
                raise ValueError("record is not an object")
            send(name, *envelope(s, payload))
            ok += 1
        except Exception as e:
            bad += 1
            send(f"{name}-dlq", None, json.dumps({"file": file.filename, "line": line_no,
                 "error": str(e), "raw": str(rec)[:2000]}).encode())
    producer.flush(30)
    return {"file": file.filename, "published": ok, "rejected_to_dlq": bad}


# ---------- consumers ----------
@app.get("/v1/streams/{name}/consumer-groups/{group}/lag", dependencies=[Depends(auth)])
def lag(name: str, group: str):
    s = get_stream(name)
    c = Consumer({"bootstrap.servers": BROKERS, "group.id": group, "enable.auto.commit": False})
    tps = c.committed([TopicPartition(name, p) for p in range(s["partitions"])], timeout=5)
    out = []
    for tp in tps:
        _, hi = c.get_watermark_offsets(tp, timeout=5)
        committed = tp.offset if tp.offset >= 0 else 0
        out.append({"partition": tp.partition, "committed": committed, "latest": hi, "lag": hi - committed})
    c.close()
    return {"group": group, "total_lag": sum(p["lag"] for p in out), "partitions": out}


@app.get("/health")
def health():
    return {"status": "ok"}


# ---------- schema registry ----------
@app.post("/v1/streams/{name}/schemas", dependencies=[Depends(auth)], status_code=201)
def add_schema(name: str, schema: dict):
    """Schema = {field: string|integer|double|boolean}. New versions must be backward compatible
    (existing fields keep their type; new fields are allowed)."""
    s = get_stream(name)
    if not schema or any(t not in TYPES for t in schema.values()):
        raise HTTPException(422, f"types must be one of {list(TYPES)}")
    old = s.get("schema") or {}
    for f, t in old.items():
        if schema.get(f) != t:
            raise HTTPException(409, f"incompatible: field '{f}' removed or type changed")
    with db() as c, c.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(version),0)+1 AS v FROM schemas WHERE stream=%s", (name,))
        v = cur.fetchone()["v"]
        cur.execute("INSERT INTO schemas(stream,version,schema) VALUES(%s,%s,%s)", (name, v, json.dumps(schema)))
    return {"stream": name, "version": v, "schema": schema}


@app.get("/v1/streams/{name}/schemas", dependencies=[Depends(auth)])
def list_schemas(name: str):
    get_stream(name)
    with db() as c, c.cursor() as cur:
        cur.execute("SELECT version, schema, created_at FROM schemas WHERE stream=%s ORDER BY version DESC", (name,))
        return cur.fetchall()


# ---------- live tail / delete ----------
@app.get("/v1/streams/{name}/peek", dependencies=[Depends(auth)])
def peek(name: str, limit: int = 20):
    s = get_stream(name)
    c = Consumer({"bootstrap.servers": BROKERS, "group.id": "_streamhub_peek", "enable.auto.commit": False})
    out = []
    for p in range(s["partitions"]):
        lo, hi = c.get_watermark_offsets(TopicPartition(name, p), timeout=5)
        start = max(lo, hi - limit)
        if hi > start:
            c.assign([TopicPartition(name, p, start)])
            for _ in range(hi - start):
                m = c.poll(2)
                if m is None or m.error():
                    break
                out.append({"partition": p, "offset": m.offset(), "event": json.loads(m.value())})
    c.close()
    out.sort(key=lambda e: e["event"]["event_time"], reverse=True)
    return out[:limit]


@app.delete("/v1/streams/{name}", dependencies=[Depends(admin_only)])
def delete_stream(name: str):
    get_stream(name)
    for f in admin.delete_topics([name, f"{name}-dlq"]).values():
        try: f.result()
        except Exception: pass
    with db() as c, c.cursor() as cur:
        cur.execute("DELETE FROM schemas WHERE stream=%s", (name,))
        cur.execute("DELETE FROM streams WHERE name=%s", (name,))
    return {"deleted": name}


# ---------- API keys (admin) ----------
class KeyIn(BaseModel):
    name: str
    role: str = Field(pattern="^(admin|producer|consumer)$")


@app.post("/v1/keys", dependencies=[Depends(admin_only)], status_code=201)
def create_key(k: KeyIn):
    key = "sh_live_" + secrets.token_urlsafe(24)
    with db() as c, c.cursor() as cur:
        cur.execute("INSERT INTO api_keys(name,role,key_hash) VALUES(%s,%s,%s) RETURNING id",
                    (k.name, k.role, hashlib.sha256(key.encode()).hexdigest()))
        kid = cur.fetchone()["id"]
    return {"id": kid, "name": k.name, "role": k.role, "api_key": key, "note": "shown once"}


@app.get("/v1/keys", dependencies=[Depends(admin_only)])
def list_keys():
    with db() as c, c.cursor() as cur:
        cur.execute("SELECT id,name,role,created_at FROM api_keys ORDER BY id")
        return cur.fetchall()


@app.delete("/v1/keys/{kid}", dependencies=[Depends(admin_only)])
def delete_key(kid: int):
    with db() as c, c.cursor() as cur:
        cur.execute("DELETE FROM api_keys WHERE id=%s", (kid,))
    return {"deleted": kid}


@app.get("/v1/whoami")
def whoami(role: str = Depends(auth)):
    return {"role": role}


app.mount("/", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static"), html=True), name="ui")
