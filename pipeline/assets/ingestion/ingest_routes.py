"""@bruin
name: public.raw_routes
type: python
connection: supabase

materialization:
  type: table

columns:

  - name: airline
    type: string
    description: "2-letter IATA or 3-letter ICAO airline code"

  - name: airline_id
    type: integer
    description: "Internal OpenFlights airline ID, -1 if unknown"

  - name: src_airport
    type: string
    description: "3-letter IATA or 4-letter ICAO source airport code"

  - name: src_airport_id
    type: integer
    description: "Internal OpenFlights source airport ID, -1 if unknown"

  - name: dst_airport
    type: string
    description: "3-letter IATA or 4-letter ICAO destination airport code"

  - name: dst_airport_id
    type: integer
    description: "Internal OpenFlights destination airport ID, -1 if unknown"

  - name: codeshare
    type: string
    description: "Y if this is a codeshare flight, empty otherwise"

  - name: stops
    type: integer
    description: "Number of stops — 0 means direct flight"

  - name: equipment
    type: string
    description: "Space-separated IATA aircraft type codes"

@bruin"""

import csv
import io

import requests

URL = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat"


def null(v):
    return None if v in (r"\N", "", "\\N") else v


def null_int(v):
    try:
        return int(v) if v not in (r"\N", "", "\\N") else None
    except Exception:
        return None


def materialize():
    print("Downloading routes...")

    r = requests.get(URL, timeout=30)
    r.raise_for_status()

    rows = list(csv.reader(io.StringIO(r.content.decode("latin-1"))))
    print(f"{len(rows):,} rows downloaded")

    records = []
    for row in rows:
        if len(row) < 9:
            continue

        # opcional: filtrar filas basura
        if row[2] in (r"\N", "", "\\N") or row[4] in (r"\N", "", "\\N"):
            continue

        records.append(
            {
                "airline": null(row[0]),
                "airline_id": null_int(row[1]),
                "src_airport": null(row[2]),
                "src_airport_id": null_int(row[3]),
                "dst_airport": null(row[4]),
                "dst_airport_id": null_int(row[5]),
                "codeshare": null(row[6]),
                "stops": null_int(row[7]),
                "equipment": null(row[8]),
            }
        )

    print(f"[DONE] {len(records):,} routes ready")

    return records
