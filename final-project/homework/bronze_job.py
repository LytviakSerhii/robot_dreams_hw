"""Bronze: landing-зона -> bronze.raw_events у Postgres. ЕТАП 1 — реалізуйте три місця з TODO.

Spark у local mode читає NDJSON із landing і дописує рядки у сховище через JDBC.

Контракт шару Bronze (повністю — SPEC.md, розділ 3):
  * читаємо з ЯВНОЮ схемою (ніякого inferSchema: схема — це контракт, а не здогадка);
  * `payload` лишається СИРИМ JSON-рядком: Bronze нічого не парсить і нічого не виправляє;
  * ідемпотентність за файлом: файл, який уже завантажено, вдруге не потрапляє;
  * атомарний append: або всі нові рядки в таблиці, або жодного.

    uv run python bronze_job.py
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from common import config
from common.spark import build_spark

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("ride_id", StringType()),
        StructField("occurred_at", StringType()),
        StructField("source", StringType()),
        StructField("payload", StringType()),
    ]
)


def read_landing(spark: SparkSession, landing_dir: str) -> DataFrame:
    raw = (
        spark.read.schema(EVENT_SCHEMA)
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.ndjson")
        .option("mode", "FAILFAST")
        .json(landing_dir)
    )

    return raw.select(
        "event_id",
        "event_type",
        "ride_id",
        F.to_timestamp("occurred_at").alias("occurred_at"),
        "source",
        "payload",
        F.regexp_extract(
            F.regexp_replace(F.col("_metadata.file_path"), r"\\", "/"),
            r"(dt=[^/]+/hour=[^/]+/[^/]+\.ndjson)$",
            1,
        ).alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
    )


def loaded_files(spark: SparkSession) -> set[str]:
    """Файли, які вже лежать у Bronze (ключ ідемпотентності). ДАНО."""
    query = f"(SELECT DISTINCT _source_file FROM {config.BRONZE_TABLE}) AS t"
    rows = spark.read.jdbc(config.JDBC_URL, query, properties=config.JDBC_PROPERTIES).collect()
    return {r["_source_file"] for r in rows}


def select_new(df: DataFrame, already_loaded: set[str]) -> DataFrame:
    if not already_loaded:
        return df

    return df.filter(~F.col("_source_file").isin(sorted(already_loaded)))


def write_bronze(df: DataFrame) -> None:
    (
        df.coalesce(1)
        .write.mode("append")
        .option("isolationLevel", "READ_COMMITTED")
        .option("batchsize", 5000)
        .jdbc(config.JDBC_URL, config.BRONZE_TABLE, properties=config.JDBC_PROPERTIES)
    )


def main() -> None:
    """ДАНО."""
    spark = build_spark("bronze-job")
    try:
        raw = read_landing(spark, str(config.LANDING_DIR))
        new = select_new(raw, loaded_files(spark)).cache()

        n_new = new.count()
        if n_new == 0:
            print("Bronze: нових файлів немає — пропускаю запис (ідемпотентно).")
        else:
            n_files = new.select("_source_file").distinct().count()
            write_bronze(new)
            print(f"Bronze: додано {n_new} подій із {n_files} файлів.")

        total = spark.read.jdbc(
            config.JDBC_URL,
            f"(SELECT count(*) AS n FROM {config.BRONZE_TABLE}) AS t",
            properties=config.JDBC_PROPERTIES,
        ).collect()[0]["n"]
        print(f"Bronze total: {total}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
