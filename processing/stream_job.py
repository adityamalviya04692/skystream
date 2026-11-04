"""SkyStream Spark Structured Streaming job.

Kafka (raw.states, raw.metar, ref.aircraft) -> Delta bronze (raw, replayable) -> Delta silver (parsed, validated,
deduplicated) -> takeoff/landing events (gold) -> Postgres serving tables for the dashboard.
Bad state vectors go to the dlq topic instead of failing the batch.
"""
import json
import os
from collections import Counter
from contextlib import contextmanager

import psycopg2
from delta.tables import DeltaTable
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import logic

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
LAKE = os.getenv("LAKE_PATH", "/lake")
TRIGGER = os.getenv("TRIGGER", "30 seconds")
AIRPORTS = logic.load_airports(os.getenv("AIRPORTS_CSV", "/reference/airports.csv"))
PG = {"host": os.getenv("PGHOST", "postgres"), "dbname": os.getenv("PGDATABASE", "skystream"),
      "user": os.getenv("PGUSER", "skystream"), "password": os.getenv("PGPASSWORD", "skystream")}

STATE_SCHEMA = ("icao24 STRING, callsign STRING, origin_country STRING, time_position LONG, last_contact LONG, "
                "longitude DOUBLE, latitude DOUBLE, baro_altitude DOUBLE, on_ground BOOLEAN, velocity DOUBLE, "
                "true_track DOUBLE, vertical_rate DOUBLE, geo_altitude DOUBLE, squawk STRING, spi BOOLEAN, "
                "position_source INT, capture_ts LONG")
AIRCRAFT_SCHEMA = ("icao24 STRING, registration STRING, typecode STRING, model STRING, manufacturer STRING, "
                   "operator STRING, operator_icao STRING, built STRING")
EVENT_SCHEMA = ("icao24 STRING, callsign STRING, airline_icao STRING, event STRING, ts LONG, airport STRING, "
                "latitude DOUBLE, longitude DOUBLE")
METAR_SCHEMA = ("icao STRING, obs_time LONG, temp_c DOUBLE, dewpoint_c DOUBLE, wind_dir STRING, wind_kt DOUBLE, "
                "visibility_m DOUBLE, wx STRING, raw STRING")

spark = (SparkSession.builder.appName("skystream")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")


def kafka_stream(topic: str):
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
            .select(F.col("key").cast("string").alias("key"), F.col("value").cast("string").alias("value"),
                    "topic", "partition", "offset", F.col("timestamp").alias("kafka_ts")))


def write_bronze(df, name: str) -> None:
    """Keep every message exactly as received, so silver and gold can always be rebuilt."""
    (df.withColumn("ingest_date", F.to_date("kafka_ts"))
       .write.format("delta").mode("append").partitionBy("ingest_date").save(f"{LAKE}/bronze/{name}"))


def merge_delta(df, path: str, keys: list) -> None:
    """Idempotent write: replaying a batch after a restart does not duplicate rows."""
    if not DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").save(path)
        return
    cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    DeltaTable.forPath(spark, path).alias("t").merge(df.alias("s"), cond).whenNotMatchedInsertAll().execute()


@contextmanager
def pg_cursor():
    """One transaction per batch: commit on success, roll back on error, always close the connection."""
    conn = psycopg2.connect(**PG)
    try:
        with conn, conn.cursor() as cur:
            yield cur
    finally:
        conn.close()


def upsert(cur, table: str, rows: list, cols: list, key: list, newer_than: str = None) -> None:
    if not rows:
        return
    updates = ", ".join(f"{c} = excluded.{c}" for c in cols if c not in key)
    guard = f" WHERE excluded.{newer_than} >= {table}.{newer_than}" if newer_than else ""
    execute_values(cur, f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s "
                        f"ON CONFLICT ({', '.join(key)}) DO UPDATE SET {updates}{guard}",
                   [tuple(r.get(c) for c in cols) for r in rows])


def process_states(df, batch_id: int) -> None:
    if df.isEmpty():
        return
    df = df.persist()  # read from Kafka once, reused by the bronze, dlq, silver and serving writes
    write_bronze(df, "states")
    parsed = df.select("value", F.from_json("value", STATE_SCHEMA).alias("s"))
    valid = (F.col("s.icao24").rlike("^[0-9a-f]{6}$")
             & F.col("s.latitude").between(-90, 90) & F.col("s.longitude").between(-180, 180))
    bad = parsed.where(~F.coalesce(valid, F.lit(False)))
    (bad.select(F.to_json(F.struct(
            F.lit("raw.states").alias("source"),
            F.when(F.col("s").isNull(), "unparseable json")
             .when(~F.coalesce(F.col("s.icao24").rlike("^[0-9a-f]{6}$"), F.lit(False)), "bad icao24")
             .otherwise("position missing or out of range").alias("reason"),
            F.col("value").alias("payload"))).alias("value"))
        .write.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP).option("topic", "dlq").save())

    good = (parsed.where(valid).select("s.*")
            .withColumn("airline_icao", F.substring("callsign", 1, 3))
            .withColumn("capture_date", F.to_date(F.from_unixtime("capture_ts")))
            .dropDuplicates(["icao24", "time_position"]))
    merge_delta(good, f"{LAKE}/silver/positions", ["icao24", "time_position"])

    rows = [r.asDict() for r in good.collect()]  # one India capture is a few hundred rows: fine on the driver
    with pg_cursor() as cur:
        cur.execute("SELECT icao24, on_ground, COALESCE(time_position, last_contact, capture_ts), latitude, longitude "
                    "FROM live_positions WHERE icao24 = ANY(%s)", ([r["icao24"] for r in rows],))
        prev = {i: {"on_ground": g, "ts": t, "lat": la, "lon": lo} for i, g, t, la, lo in cur.fetchall()}
        events, latest = logic.detect_events(prev, rows, AIRPORTS)
        for r in latest:
            r["airline_icao"] = logic.airline_of(r.get("callsign"))
        upsert(cur, "live_positions", latest,
               ["icao24", "callsign", "airline_icao", "origin_country", "latitude", "longitude", "baro_altitude",
                "geo_altitude", "velocity", "true_track", "vertical_rate", "on_ground", "squawk", "time_position",
                "last_contact", "capture_ts"], ["icao24"], newer_than="capture_ts")
        seen, airborne = Counter(r["capture_ts"] for r in rows), Counter(r["capture_ts"] for r in rows if r["on_ground"] is False)
        execute_values(cur, "INSERT INTO capture_batches (batch_id, capture_ts, aircraft, airborne) VALUES %s "
                            "ON CONFLICT DO NOTHING", [(batch_id, ts, n, airborne[ts]) for ts, n in seen.items()])
        if events:
            execute_values(cur, "INSERT INTO flight_events (icao24, callsign, airline_icao, event, ts, airport, latitude, "
                                "longitude) VALUES %s ON CONFLICT DO NOTHING",
                           [tuple(e[c] for c in ("icao24", "callsign", "airline_icao", "event", "ts", "airport",
                                                 "latitude", "longitude")) for e in events])
    if events:
        merge_delta(spark.createDataFrame([tuple(e.values()) for e in events], EVENT_SCHEMA),
                    f"{LAKE}/gold/flight_events", ["icao24", "event", "ts"])
    df.unpersist()
    print(f"[states] batch {batch_id}: {len(rows)} positions, {len(events)} events")


def process_metar(df, batch_id: int) -> None:
    if df.isEmpty():
        return
    write_bronze(df, "metar")
    rows = [logic.metar_row(json.loads(v)) for (v,) in df.select("value").collect()]
    rows = [r for r in rows if r["icao"] and r["obs_time"]]
    cols = list(rows[0]) if rows else []
    with pg_cursor() as cur:
        upsert(cur, "airport_weather", rows, cols, ["icao"], newer_than="obs_time")
        if rows:
            execute_values(cur, f"INSERT INTO airport_weather_obs ({', '.join(cols)}) VALUES %s ON CONFLICT DO NOTHING",
                           [tuple(r[c] for c in cols) for r in rows])
    if rows:
        merge_delta(spark.createDataFrame([tuple(r[c] for c in cols) for r in rows], METAR_SCHEMA),
                    f"{LAKE}/silver/metar", ["icao", "obs_time"])
    print(f"[metar] batch {batch_id}: {len(rows)} observations")


def process_aircraft(df, batch_id: int) -> None:
    if df.isEmpty():
        return
    write_bronze(df, "aircraft")
    parsed = df.select(F.from_json("value", AIRCRAFT_SCHEMA).alias("a")).select("a.*").where("icao24 IS NOT NULL")
    rows = [r.asDict() for r in parsed.dropDuplicates(["icao24"]).collect()]
    with pg_cursor() as cur:
        upsert(cur, "aircraft", rows, ["icao24", "registration", "typecode", "model", "manufacturer", "operator",
                                       "operator_icao", "built"], ["icao24"])
    print(f"[aircraft] batch {batch_id}: {len(rows)} aircraft")


for topic, fn, name in (("raw.states", process_states, "states"), ("raw.metar", process_metar, "metar"),
                        ("ref.aircraft", process_aircraft, "aircraft")):
    (kafka_stream(topic).writeStream.foreachBatch(fn).trigger(processingTime=TRIGGER)
     .option("checkpointLocation", f"{LAKE}/_checkpoints/{name}").start())

spark.streams.awaitAnyTermination()
