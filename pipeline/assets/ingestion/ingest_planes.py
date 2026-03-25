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

import requests

URL = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/planes.dat"


def null(v):
    return None if v in (r"\N", "", "\\N") else v


def materialize():
    print("Downloading planes...")

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
