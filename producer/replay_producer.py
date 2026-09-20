
import csv
import json
import time
from pathlib import Path
from kafka import KafkaProducer


def read_manifest(manifest_path):
    """Return source files in reproducible block order."""
    manifest_path = Path(manifest_path)
    dataset_root = Path("/mnt/e/London-Smart-Meter-Dataset")

    files = []

    with manifest_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            files.append(
                (
                    int(row["block_id"]),
                    dataset_root / row["relative_path"]
                )
            )

    return [
        path
        for _, path in sorted(files, key=lambda x: x[0])
    ]


def iter_records(source_files):
    """Yield historical observations without modifying raw files."""
    for source_file in source_files:

        with source_file.open(
            "r",
            encoding="utf-8",
            newline=""
        ) as f:

            reader = csv.DictReader(f)

            for row in reader:
                yield {
                    "LCLid": row["LCLid"],
                    "tstp": row["tstp"],
                    "energy(kWh/hh)": row["energy(kWh/hh)"]
                }


def replay(
    manifest_path,
    topic="smart-meter-readings",
    bootstrap_servers="localhost:9092",
    max_records=None,
    replay_delay=0.0,
    dry_run=False
):
    """
    Replay historical smart-meter observations.

    dry_run=True validates replay without Kafka.
    """

    source_files = read_manifest(manifest_path)

    producer = None

    if not dry_run:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda value: json.dumps(
                value
            ).encode("utf-8")
        )

    sent = 0

    try:
        for record in iter_records(source_files):

            if dry_run:
                if sent < 5:
                    print(json.dumps(record))
            else:
                producer.send(topic, value=record)

            sent += 1

            if replay_delay > 0:
                time.sleep(replay_delay)

            if (
                max_records is not None
                and sent >= max_records
            ):
                break

    finally:
        if producer is not None:
            producer.flush()
            producer.close()

    return sent


if __name__ == "__main__":

    manifest = Path(
        "/mnt/e/smart-meter-bigdata-project/"
        "data/replay/replay_manifest.csv"
    )

    count = replay(
        manifest_path=manifest,
        max_records=10,
        replay_delay=0.0,
        dry_run=True
    )

    print(f"\nDry-run records: {count}")
