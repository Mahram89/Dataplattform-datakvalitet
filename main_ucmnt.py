# Developer/Author: Mahram S.
# Everyone can use it.
# I removed the comments
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

import polars as pl

REQUIRED_COLUMNS = ["person_id", "name", "age", "city"]


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )


def validate_columns(df: pl.DataFrame) -> None:
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Mangler påkrevde kolonner: {missing}")


def clean_data(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    df = df.with_columns([
        pl.col("person_id").cast(pl.Utf8, strict=False),
        pl.col("name").cast(pl.Utf8, strict=False),
        pl.col("age").cast(pl.Int64, strict=False),
        pl.col("city").cast(pl.Utf8, strict=False),
    ])

    invalid_rows = df.filter(
        pl.col("person_id").is_null() |
        pl.col("name").is_null() |
        pl.col("age").is_null() |
        (pl.col("age") < 0) |
        (pl.col("age") > 120) |
        pl.col("city").is_null()
    )

    valid_rows = df.filter(
        pl.col("person_id").is_not_null() &
        pl.col("name").is_not_null() &
        pl.col("age").is_not_null() &
        (pl.col("age") >= 0) &
        (pl.col("age") <= 120) &
        pl.col("city").is_not_null()
    )

    return valid_rows, invalid_rows


def process_file(input_file: Path, output_dir: Path) -> dict:
    logging.info("Leser fil: %s", input_file)

    df = pl.read_csv(input_file)
    validate_columns(df)

    valid_df, invalid_df = clean_data(df)

    output_dir.mkdir(parents=True, exist_ok=True)

    parquet_file = output_dir / f"{input_file.stem}.parquet"
    invalid_file = output_dir / f"{input_file.stem}_invalid.csv"
    report_file = output_dir / f"{input_file.stem}_report.json"

    valid_df.write_parquet(parquet_file)
    invalid_df.write_csv(invalid_file)

    report = {
        "input_file": str(input_file),
        "total_rows": df.height,
        "valid_rows": valid_df.height,
        "invalid_rows": invalid_df.height,
        "output_parquet": str(parquet_file),
        "invalid_rows_file": str(invalid_file),
    }

    report_file.write_text(json.dumps(report, indent=2), encoding="utf 8")

    logging.info("Ferdig. Gyldige rader: %s, ugyldige rader: %s", valid_df.height, invalid_df.height)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Prosesser CSV til Parquet med validering")
    parser.add_argument("--input", required=True, help="Input CSV fil")
    parser.add_argument("--output-dir", required=True, help="Output katalog")
    args = parser.parse_args()

    setup_logging()

    input_file = Path(args.input)
    output_dir = Path(args.output_dir)

    if not input_file.exists():
        raise FileNotFoundError(f"Fant ikke inputfil: {input_file}")

    report = process_file(input_file, output_dir)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()