# Developer/Author: Mahram S.
# Everyone can use it

from __future__ import annotations

##standardbiblioteker for henholdsvis håndtering av CLI-argumenter, JSON-data, logging og filstier på en ryddig måte
import argparse
import json
import logging
from pathlib import Path

import polars as pl     ## rask databehandling til aa lese, filtrere og skrive data som CSV og Parquet

REQUIRED_COLUMNS = ["person_id", "name", "age", "city"]

# Definerer funksjon for sette opp logging 
def setup_logging() -> None:    #  -> None betyr at funksjonen ikke returnerer noen verdi
    logging.basicConfig(        # konfiurerer global logging
        level=logging.INFO,     # Setter loggingnivaa til info over
        format="%(asctime)s %(levelname)s %(message)s"  # # Format: tid, nivå, melding
    )

# Funksjon som sjekker at nødvendige kolonner finnes
# df er en variabel som representerer en DataFrame
def validate_columns(df: pl.DataFrame) -> None:
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]    # Lager liste over manglende kolonner
    if missing: # Hvis noen kolonner mangler
        raise ValueError(f"Mangler paakrevde kolonner: {missing}")  # Stopper programmet med feilmelding

# En tuple er en samling av flere verdier i én variabel
def clean_data(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:  # Returnerer to DataFrames: gyldige og ugyldige rader
    df = df.with_columns([  # Endrer kolonner i DataFrame
        pl.col("person_id").cast(pl.Utf8, strict=False),  # Konverterer til tekst, ignorerer feil
        pl.col("name").cast(pl.Utf8, strict=False),       # Konverterer til tekst
        pl.col("age").cast(pl.Int64, strict=False),       # Konverterer til heltall
        pl.col("city").cast(pl.Utf8, strict=False),       # Konverterer til tekst
    ])

    invalid_rows = df.filter(  # Filtrerer ut rader som er ugyldige
        pl.col("person_id").is_null() |  # Mangler person_id
        pl.col("name").is_null() |       # Mangler navn
        pl.col("age").is_null() |        # Mangler alder
        (pl.col("age") < 0) |            # Alder er negativ
        (pl.col("age") > 120) |          # Alder er urealistisk høy
        pl.col("city").is_null()         # Mangler by
    )

    valid_rows = df.filter(  # Filtrerer ut rader som er gyldige
        pl.col("person_id").is_not_null() &  # Har person_id
        pl.col("name").is_not_null() &       # Har navn
        pl.col("age").is_not_null() &        # Har alder
        (pl.col("age") >= 0) &               # Alder er gyldig (>= 0)
        (pl.col("age") <= 120) &             # Alder er realistisk (<= 120)
        pl.col("city").is_not_null()         # Har by
    )

    return valid_rows, invalid_rows  # Returnerer begge DataFrames som en tuple: først gyldige, så ugyldige rader

# -> dict betyr at funksjonen returnerer en dictionary.
def process_file(input_file: Path, output_dir: Path) -> dict:  # Funksjon som prosesserer én fil og returnerer en rapport som dict
    logging.info("Leser fil: %s", input_file)  # Logger hvilken fil som leses

    df = pl.read_csv(input_file)  # Leser CSV-fil inn i en DataFrame
    validate_columns(df)  # Sjekker at nødvendige kolonner finnes

    valid_df, invalid_df = clean_data(df)  # Pakker ut tuple til to variabler (gyldige og ugyldige rader)

    output_dir.mkdir(parents=True, exist_ok=True)  # Lager mappe hvis den ikke finnes, uten feil hvis den finnes

    parquet_file = output_dir / f"{input_file.stem}.parquet"  # Lager filsti ved å kombinere mappe og filnavn
    invalid_file = output_dir / f"{input_file.stem}_invalid.csv"  # Samme, men for ugyldige rader
    report_file = output_dir / f"{input_file.stem}_report.json"  # Filsti for rapport

    valid_df.write_parquet(parquet_file)  # Skriver gyldige data til Parquet-fil
    invalid_df.write_csv(invalid_file)    # Skriver ugyldige data til CSV-fil

    report = {  # Lager en dictionary med informasjon om kjøringen
        "input_file": str(input_file),        # Path til inputfil som tekst
        "total_rows": df.height,              # Antall rader i original data
        "valid_rows": valid_df.height,        # Antall gyldige rader
        "invalid_rows": invalid_df.height,    # Antall ugyldige rader
        "output_parquet": str(parquet_file),  # Filsti til lagret Parquet-fil
        "invalid_rows_file": str(invalid_file),  # Filsti til CSV med ugyldige rader
    }

    report_file.write_text(json.dumps(report, indent=2), encoding="utf-8")  # skriver dict til JSON-fil, pen format

    logging.info("Ferdig. Gyldige rader: %s, ugyldige rader: %s", valid_df.height, invalid_df.height)  # logger resultat

    return report  # returnerer rapport (dict)

# Parseren leser og tolker argumenter du skriver i terminalen
def main() -> None:  # hovedfunksjon, returnerer ingenting
    parser = argparse.ArgumentParser(description="Prosesser CSV til Parquet med validering")  # lager CLI parser
    parser.add_argument("--input", required=True, help="Input CSV fil")  # forventer input argument
    parser.add_argument("--output-dir", required=True, help="Output katalog")  # forventer output mappe
    args = parser.parse_args()  # leser argumenter fra terminalen

    setup_logging()  # starter logging

    input_file = Path(args.input)  # lager Path objekt fra input
    output_dir = Path(args.output_dir)  # lager Path objekt fra output mappe

    if not input_file.exists():  # sjekker om fil finnes
        raise FileNotFoundError(f"Fant ikke inputfil: {input_file}")  # stopper hvis ikke finnes

    report = process_file(input_file, output_dir)  # kjører hovedlogikk
    print(json.dumps(report, indent=2))  # skriver rapport til terminal

if __name__ == "__main__":  # kjører kun hvis filen startes direkte
    main()  # starter programmet