"""Convert TweetClaw exports into the project processed tweet schema."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import Any

WRAPPER_KEYS = ("tweets", "items", "data", "results", "posts")
TEXT_KEYS = ("text_content", "full_text", "text", "tweet_text", "content")
ID_KEYS = ("tweet_id", "id", "id_str", "tweetId")
URL_KEYS = ("tweet_url", "url", "permalink", "link")
TIME_KEYS = ("timestamp_full", "created_at", "createdAt", "date", "time", "timestamp")
AUTHOR_KEYS = ("author", "user", "username", "screen_name", "handle", "user_handle")
FULLNAME_KEYS = ("fullname", "full_name", "name", "display_name")
SOURCE_KEYS = ("source_job", "query", "keyword", "search", "search_term", "topic")
METRIC_KEYS = {
    "replies": ("replies", "reply_count", "replyCount"),
    "retweets": ("retweets", "retweet_count", "retweetCount"),
    "likes": ("likes", "favorite_count", "favoriteCount", "like_count", "likeCount"),
    "views": ("views", "view_count", "viewCount"),
}
CSV_COLUMNS = (
    "tweet_id",
    "tweet_url",
    "username",
    "user_handle",
    "fullname",
    "text_content",
    "timestamp_full",
    "date_text",
    "engagement_stats.replies",
    "engagement_stats.retweets",
    "engagement_stats.likes",
    "engagement_stats.views",
    "source_job",
    "job_type",
)


def read_export(path: Path) -> list[dict[str, Any]]:
    """Read TweetClaw CSV, JSONL, or JSON records."""
    suffix = path.suffix.lower()
    if suffix == ".csv":
        with path.open(newline="", encoding="utf-8") as handle:
            return [dict(row) for row in csv.DictReader(handle)]

    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []

    if suffix == ".jsonl":
        records = []
        for line_number, line in enumerate(text.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as error:
                exit_json_error(path, error, line_number)
            if isinstance(value, dict):
                records.append(value)
        return records

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as error:
        exit_json_error(path, error)
    return list(unwrap_records(parsed))


def exit_json_error(path: Path, error: json.JSONDecodeError, line_number: int | None = None) -> None:
    """Print a readable JSON parse error and stop."""
    location = f"{path}:{line_number}" if line_number is not None else str(path)
    print(f"Invalid JSON in {location}: {error}", file=sys.stderr)
    raise SystemExit(1)


def unwrap_records(value: Any) -> Iterable[dict[str, Any]]:
    """Yield tweet dictionaries from common export wrapper shapes."""
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                yield item
        return

    if isinstance(value, dict):
        for key in WRAPPER_KEYS:
            wrapped = value.get(key)
            if isinstance(wrapped, list):
                yield from unwrap_records(wrapped)
                return
        yield value


def first_text(record: dict[str, Any], keys: tuple[str, ...]) -> str:
    """Return the first non-empty string-like field."""
    for key in keys:
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return " ".join(value.split())
        if isinstance(value, int):
            return str(value)
    return ""


def nested_author(record: dict[str, Any]) -> dict[str, Any]:
    """Return a nested author object when the export provides one."""
    for key in ("author", "user"):
        value = record.get(key)
        if isinstance(value, dict):
            return value
    return {}


def normalize_handle(value: str) -> str:
    """Return a handle without a leading at sign."""
    return value.strip().removeprefix("@")


def author_handle(record: dict[str, Any]) -> str:
    """Resolve the author handle from flat or nested export fields."""
    nested = nested_author(record)
    value = first_text(nested, AUTHOR_KEYS) or first_text(record, AUTHOR_KEYS)
    return normalize_handle(value)


def author_name(record: dict[str, Any]) -> str:
    """Resolve the author display name from flat or nested export fields."""
    nested = nested_author(record)
    return first_text(nested, FULLNAME_KEYS) or first_text(record, FULLNAME_KEYS)


def metric_value(record: dict[str, Any], metric: str) -> str:
    """Resolve a metric from flat fields or common nested metric containers."""
    for container_key in ("engagement_stats", "metrics", "public_metrics"):
        container = record.get(container_key)
        if isinstance(container, dict):
            value = first_text(container, METRIC_KEYS[metric])
            if value:
                return value
    return first_text(record, METRIC_KEYS[metric])


def status_url(record: dict[str, Any], tweet_id: str, handle: str) -> str:
    """Return an existing URL or build an X status URL when possible."""
    url = first_text(record, URL_KEYS)
    if url:
        return url
    if tweet_id and handle:
        return f"https://x.com/{handle}/status/{tweet_id}"
    return ""


def normalize_record(record: dict[str, Any], fallback_source: str) -> dict[str, str] | None:
    """Map one TweetClaw record to the processed output schema."""
    text = first_text(record, TEXT_KEYS)
    if not text:
        return None

    tweet_id = first_text(record, ID_KEYS).removeprefix("'")
    handle = author_handle(record)
    timestamp = first_text(record, TIME_KEYS)

    return {
        "tweet_id": tweet_id,
        "tweet_url": status_url(record, tweet_id, handle),
        "username": handle,
        "user_handle": handle,
        "fullname": author_name(record),
        "text_content": text,
        "timestamp_full": timestamp,
        "date_text": timestamp.split("T", maxsplit=1)[0] if "T" in timestamp else timestamp,
        "engagement_stats.replies": metric_value(record, "replies"),
        "engagement_stats.retweets": metric_value(record, "retweets"),
        "engagement_stats.likes": metric_value(record, "likes"),
        "engagement_stats.views": metric_value(record, "views"),
        "source_job": first_text(record, SOURCE_KEYS) or fallback_source,
        "job_type": "TweetClaw Import",
    }


def dedupe_rows(rows: Iterable[dict[str, str]]) -> list[dict[str, str]]:
    """Deduplicate rows by tweet id when available, otherwise by text."""
    seen: set[str] = set()
    unique_rows = []
    for row in rows:
        key = row["tweet_id"] or row["text_content"]
        if key in seen:
            continue
        seen.add(key)
        unique_rows.append(row)
    return unique_rows


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    """Write normalized rows as a processed CSV file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, rows: list[dict[str, str]]) -> None:
    """Write normalized rows as JSON for inspection or downstream use."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Convert a saved TweetClaw export into this project's processed tweet CSV schema."
    )
    parser.add_argument("input", type=Path, help="TweetClaw JSON, JSONL, or CSV export")
    parser.add_argument("output_csv", type=Path, help="Processed CSV path to write")
    parser.add_argument("--json-output", type=Path, help="Optional normalized JSON output path")
    parser.add_argument("--source-job", default="tweetclaw", help="Fallback source_job value")
    return parser.parse_args()


def main() -> None:
    """Run the conversion command."""
    args = parse_args()
    rows = [
        row
        for record in read_export(args.input)
        if (row := normalize_record(record, args.source_job)) is not None
    ]
    rows = dedupe_rows(rows)
    write_csv(args.output_csv, rows)
    if args.json_output is not None:
        write_json(args.json_output, rows)
    print(f"Wrote {len(rows)} TweetClaw rows to {args.output_csv}")


if __name__ == "__main__":
    main()
