from __future__ import annotations

import polars as pl

from . import config


def build_repo_activity(silver: pl.DataFrame) -> pl.DataFrame:
    df = silver.group_by("repo_name").agg(
        [
            pl.count("event_id").alias("event_count").cast(pl.Int64),
            pl.n_unique("event_type").alias("distinct_event_types").cast(pl.Int64)
        ]
    ).sort("event_count", descending=True)

    df.write_parquet(config.GOLD_REPO_ACTIVITY, mkdir=True)


def build_activity_per_minute(silver: pl.DataFrame) -> pl.DataFrame:
    df = (
        silver
        .with_columns(pl.col("created_at").dt.truncate("1m").alias("minute"))
        .group_by("minute")
        .agg(pl.count("event_id").alias("event_count").cast(pl.Int64))
        .sort("minute")
    )

    df.write_parquet(config.GOLD_ACTIVITY_PER_MINUTE, mkdir=True)


def build_push_commits_by_repo(silver: pl.DataFrame) -> pl.DataFrame:
    df = (
        silver
        .filter(pl.col("event_type") == "PushEvent")
        .group_by("repo_name")
        .agg([
            pl.count("event_id").alias("push_events").cast(pl.Int64),
            pl.sum("commit_count").alias("total_commits").cast(pl.Int64)
        ])
    )

    df.write_parquet(config.GOLD_PUSH_COMMITS, mkdir=True)
