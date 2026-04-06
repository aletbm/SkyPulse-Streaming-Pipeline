"""@bruin
name: public.raw_airports
type: python
connection: supabase

materialization:
  type: table

secrets:
  - key: SUPABASE_HOST
  - key: SUPABASE_PORT
  - key: SUPABASE_USER
  - key: SUPABASE_PASSWORD
  - key: SUPABASE_DATABASE

columns:

  - name: airport_id
    type: integer
    description: "Internal OpenFlights airport ID"
    primary_key: true

  - name: name
    type: string
    description: "Full airport name"

  - name: city
    type: string
    description: "City served by the airport"

  - name: country
    type: string
    description: "Country where the airport is located"

  - name: iata
    type: string
    description: "3-letter IATA code, null if not assigned"

  - name: icao
    type: string
    description: "4-letter ICAO code, null if not assigned"

  - name: latitude
    type: float
    description: "Decimal latitude of the airport"

  - name: longitude
    type: float
    description: "Decimal longitude of the airport"

  - name: altitude_ft
    type: integer
    description: "Altitude in feet above sea level"

  - name: timezone_offset
    type: float
    description: "Hours offset from UTC"

  - name: dst
    type: string
    description: "DST rule."

  - name: tz_database
    type: string
    description: "IANA tz database timezone string"

  - name: airport_type
    type: string
    description: "airport, station, port, or unknown"

  - name: source
    type: string
    description: "Data source: OurAirports, Legacy, or User"

@bruin"""

import csv
import io
import os

import psycopg2
import requests

URL = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"


def null(v):
    return None if v in (r"\N", "", "\\N") else v


def null_int(v):
    try:
        return int(v) if v not in (r"\N", "", "\\N") else None
    except Exception:
        return None


def null_float(v):
    try:
        return float(v) if v not in (r"\N", "", "\\N") else None
    except Exception:
        return None


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
    if already_populated("public.raw_airports"):
        print("Table already populated, skipping.")
        return []

    r = requests.get(URL, timeout=30)
    r.raise_for_status()

    rows = list(csv.reader(io.StringIO(r.content.decode("latin-1"))))
    print(f"{len(rows):,} rows downloaded")

    records = []
    for row in rows:
        if len(row) < 14:
            continue

        records.append(
            {
                "airport_id": null_int(row[0]),
                "name": null(row[1]),
                "city": null(row[2]),
                "country": null(row[3]),
                "iata": null(row[4]),
                "icao": null(row[5]),
                "latitude": null_float(row[6]),
                "longitude": null_float(row[7]),
                "altitude_ft": null_int(row[8]),
                "timezone_offset": null_float(row[9]),
                "dst": null(row[10]),
                "tz_database": null(row[11]),
                "airport_type": null(row[12]),
                "source": null(row[13]),
            }
        )

    print(f"[DONE] {len(records):,} airports ready")

    return records
