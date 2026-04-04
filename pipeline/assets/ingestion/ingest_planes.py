"""@bruin
name: public.raw_planes
type: python
connection: supabase

materialization:
  type: table

columns:

  - name: name
    type: string
    description: "Full aircraft type name (e.g. Boeing 737-300)"

  - name: iata
    type: string
    description: "3-letter IATA aircraft type code"

  - name: icao
    type: string
    description: "4-letter ICAO aircraft type code"

@bruin"""

import csv
import io
import os

import psycopg2
import requests

URL = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/planes.dat"


def null(v):
    return None if v in (r"\N", "", "\\N") else v


def already_populated(table: str) -> bool:
    conn = psycopg2.connect(
        host=os.environ["SUPABASE_HOST"],
        port=os.environ["SUPABASE_PORT"],
        user=os.environ["SUPABASE_USER"],
        password=os.environ["SUPABASE_PASSWORD"],
        dbname=os.environ["SUPABASE_DATABASE"],
    )
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        return count > 0
    except psycopg2.errors.UndefinedTable:
        return False
    finally:
        conn.close()


def materialize():
    if already_populated("public.raw_planes"):
        print("Table already populated, skipping.")
        return []

    r = requests.get(URL, timeout=30)
    r.raise_for_status()

    rows = list(csv.reader(io.StringIO(r.content.decode("latin-1"))))
    print(f"{len(rows):,} rows downloaded")

    records = []
    for row in rows:
        if len(row) < 3:
            continue

        records.append(
            {
                "name": null(row[0]),
                "iata": null(row[1]),
                "icao": null(row[2]),
            }
        )

    print(f"[DONE] {len(records):,} planes ready")

    return records
