"""@bruin
name: public.raw_airlines
type: python
connection: supabase

materialization:
  type: table

columns:
  - name: airline_id
    type: integer
    description: "Internal OpenFlights airline ID"
    primary_key: true

  - name: name
    type: string
    description: "Full airline name"

  - name: alias
    type: string
    description: "Alias or also-known-as name, null if unknown"

  - name: iata
    type: string
    description: "2-letter IATA code, null if not assigned"

  - name: icao
    type: string
    description: "3-letter ICAO code, null if not assigned"

  - name: callsign
    type: string
    description: "Radio telephony callsign"

  - name: country
    type: string
    description: "Country or territory where airline is incorporated"

  - name: active
    type: string
    description: "Y = currently active, N = defunct"

@bruin"""

import csv
import io

import requests

URL = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airlines.dat"


def null(v):
    return None if v in (r"\N", "", "\\N") else v


def null_int(v):
    try:
        return int(v) if v not in (r"\N", "", "\\N") else None
    except Exception:
        return None


def materialize():
    print("Downloading airlines...")

    r = requests.get(URL, timeout=30)
    r.raise_for_status()

    rows = list(csv.reader(io.StringIO(r.content.decode("latin-1"))))
    print(f"{len(rows):,} rows downloaded")

    records = []
    for row in rows:
        if len(row) < 8:
            continue

        records.append(
            {
                "airline_id": null_int(row[0]),
                "name": null(row[1]),
                "alias": null(row[2]),
                "iata": null(row[3]),
                "icao": null(row[4]),
                "callsign": null(row[5]),
                "country": null(row[6]),
                "active": null(row[7]),
            }
        )

    print(f"[DONE] {len(records):,} airlines ready")

    return records
