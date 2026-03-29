<div align="center">
    <img width=400px src=./assets/images/skypulse_logo.svg>
    <h1> SkyPulse - Streaming Pipeline </h1>
    <strong>Real-time ingestion, processing, and enrichment of global flight, weather, and seismic data - from raw API feeds to analytics-ready marts.</strong>
    <br>
    <br>
    <img src=https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white>
    <img src=https://img.shields.io/badge/UV-000000?style=for-the-badge&logo=astral&logoColor=white>
    <img src=https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white>
    <img src=https://img.shields.io/badge/NumPy-013243?style=for-the-badge&logo=numpy&logoColor=white>
    <img src=https://img.shields.io/badge/Pydantic-E92063?style=for-the-badge&logo=pydantic&logoColor=white>
    <img src=https://img.shields.io/badge/Apache%20Flink-E6526F?style=for-the-badge&logo=apacheflink&logoColor=white>
    <img src=https://img.shields.io/badge/Redpanda-FF3C3C?style=for-the-badge&logo=redpanda&logoColor=white>
    <img src=https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white>
    <img src=https://img.shields.io/badge/Supabase-3ECF8E?style=for-the-badge&logo=supabase&logoColor=white>
    <img src=https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white>
    <img src=https://img.shields.io/badge/GitHub%20Actions-2088FF?style=for-the-badge&logo=githubactions&logoColor=white>
    <img src=https://img.shields.io/badge/Terraform-7B42BC?style=for-the-badge&logo=terraform&logoColor=white>
    <img src=https://img.shields.io/badge/Ruff-D7FF64?style=for-the-badge&logo=ruff&logoColor=black>
</div>

---

## About the Project

SkyPulse is an end-to-end streaming data pipeline that continuously ingests three independent real-world data streams:

- **Flight positions** - live aircraft states from the [OpenSky Network](https://opensky-network.org/) REST API (ICAO 24-bit transponder data, ~90s polling cadence)
- **Weather snapshots** - current atmospheric conditions across a global grid of ~500+ points sourced from the [Open-Meteo](https://open-meteo.com/) API (15-min cadence)
- **Seismic events** - real-time earthquake feeds from the [USGS Earthquake Hazards Program](https://earthquake.usgs.gov/) (60s polling, event-time deduplication)

Each stream is independently produced into a Redpanda (Kafka-compatible) broker, consumed into a Supabase (PostgreSQL) landing zone, and processed by Apache Flink tumbling-window jobs that compute 5-minute aggregates. A Bruin pipeline then transforms raw landing tables into a layered analytical model (staging → intermediate → marts), producing enriched, cross-stream outputs including a composite geospatial risk score per 10-degree grid cell.

The combination of these three domains makes real-time enrichment genuinely meaningful: a grid cell with high air traffic, a nearby M6+ earthquake, and storm-level wind gusts tells a different operational story than any single stream in isolation.

---

## Objective

The project is designed around three technical goals:

**Scalability.** Producers, consumers, and Flink jobs run as independent processes with no shared state. Redpanda handles backpressure. Flink checkpointing (10s interval) ensures exactly-once semantics for the JDBC sinks. Batch sizes and flush intervals are tunable per consumer.

**Automation.** A `Makefile` orchestrates the full lifecycle: infrastructure provisioning, topic management, and launching all six streaming processes (3 producers + 3 consumers) plus four Flink jobs in a single `make streaming` target. A GitHub Actions CI pipeline enforces linting on every push to `develop` and `main`.

**Observability.** Structured logging is implemented across all producers, consumers, and Flink jobs. Flink's web UI (`:8081`) exposes job graphs, checkpoint metrics, and backpressure indicators. Bruin column-level checks and custom `row_count_positive` assertions validate data quality at every transformation layer.

---

## Tech Stack

| Category | Tools |
|---|---|
| **Language** | Python 3.12, SQL |
| **Dependency Management** | `uv` |
| **Streaming Broker** | Redpanda v25.3.9 (Kafka-compatible) |
| **Stream Processing** | Apache Flink 2.2.0 + PyFlink |
| **Landing & Serving DB** | Supabase (PostgreSQL 18 via `psycopg2`) |
| **Transformation Pipeline** | Bruin |
| **Infrastructure as Code** | Terraform (`supabase/supabase` provider) |
| **Containerization** | Docker, Docker Compose |
| **CI/CD** | GitHub Actions |
| **Linting** | Ruff, pre-commit |
| **Data Validation** | Bruin column checks + custom SQL assertions |
| **External APIs** | OpenSky Network, Open-Meteo, USGS Earthquake Feeds, OpenFlights (airports, airlines, planes) |

---

## Architecture

The pipeline is organized into four logical layers:

### 0. Infrastructure as Code (Terraform + Migrations)

Before any data flows, the entire Supabase cloud environment is provisioned and configured declaratively from `infra/`:

```
infra/
├── setup.bat                  # One-command provisioning entrypoint (Windows)
├── migrations/
│   └── migrate.py             # Python migration runner (DDL + RLS + Realtime + .env)
└── terraform/
    ├── provider.tf            # supabase/supabase provider (~> 1.0)
    ├── project.tf             # supabase_project resource (name, region, password)
    ├── settings.tf            # API settings: exposed schemas, max_rows
    ├── variables.tf           # access_token, org_id, region, db_password (all sensitive)
    └── outputs.tf             # project_ref, project_url (consumed by migrate.py)
```

**Terraform** (`infra/terraform/`) provisions the Supabase project itself via the official `supabase/supabase` provider. It creates the project in the target organization and region, and configures the PostgREST API layer to expose the `public`, `staging`, `intermediate`, and `mart` schemas. The `project_ref` and connection credentials are emitted as outputs.

**`migrate.py`** (`infra/migrations/`) reads those Terraform outputs directly via `terraform output -json` and then sequentially:

1. **Waits** for the newly-provisioned database to become reachable (up to 10 retries, 15s apart — Supabase projects take ~1 minute to boot)
2. **Runs DDL migrations** — creates PostGIS extensions, all four schemas (`staging`, `intermediate`, `mart`, `public`), grants `USAGE` to PostgREST roles, and creates all landing + tumbling window tables with their primary keys
3. **Configures Row Level Security** — enables RLS on all 26 tables across all schemas and creates two policies per table: `service_role` (full read/write) and `anon/authenticated` (read-only SELECT)
4. **Enables Supabase Realtime** — adds all public tables to the `supabase_realtime` publication
5. **Patches `.env`** — writes the correct `SUPABASE_HOST`, `SUPABASE_USER`, `SUPABASE_PASSWORD`, etc. back to the root `.env` file so the rest of the stack can connect immediately

The entire sequence runs as a single `make infra-deploy` target (which calls `infra/setup.bat`), meaning a fresh environment goes from zero to fully-wired Supabase in one command.

### 1. Ingestion (Producers)

Three independent Python producers run continuously and publish JSON-serialized messages to dedicated Redpanda topics:

| Producer | Topic | Source | Cadence |
|---|---|---|---|
| `flight_producer.py` | `flight-feeds` | OpenSky Network API (OAuth2) | 90s |
| `weather_producer.py` | `weather-feeds` | Open-Meteo API (~500 grid points) | 600s |
| `seismic_producer.py` | `earthquake-feeds` | USGS GeoJSON feeds | 60s |

Each producer uses a Pydantic-backed dataclass model (`Flight`, `Weather`, `Earthquake`) for parsing and serialization. The seismic producer implements state-based deduplication via a local JSON file (`state_seismic.json`) to avoid re-publishing events already seen in the USGS daily feed. The flight producer handles OAuth2 token refresh automatically, with rate-limit backoff on HTTP 429.

### 2. Landing (Consumers → Supabase `public` schema)

Three Kafka consumers write raw records to Supabase, one per topic:

| Consumer | Target Table | Strategy |
|---|---|---|
| `flight_consumer.py` | `public.flights` | Batch upsert (9,000 records), `ON CONFLICT (icao24) DO UPDATE` |
| `weather_consumer.py` | `public.weather` | Batch upsert (257 records), `ON CONFLICT (latitude, longitude) DO UPDATE` |
| `seismic_consumer.py` | `public.seismics` | Row-by-row insert (append-only) |

Flights and weather are upserted because they represent the latest known state of a moving object or grid point. Seismic events are append-only since each earthquake is a distinct occurrence.

**Static reference ingestion via Bruin.** Alongside the three real-time streams, Bruin is also responsible for loading static reference data from [OpenFlights](https://openflights.org/) into the `public` schema. Four Bruin Python assets (`ingest_airports.py`, `ingest_airlines.py`, `ingest_planes.py`, `ingest_routes.py`) fetch CSV data directly from the OpenFlights GitHub repository and materialize it as tables in Supabase:

| Bruin Asset | Target Table | Records |
|---|---|---|
| `ingest_airports.py` | `public.raw_airports` | ~7,700 airports worldwide |
| `ingest_airlines.py` | `public.raw_airlines` | ~5,800 airlines |
| `ingest_planes.py` | `public.raw_planes` | ~200 aircraft types |
| `ingest_routes.py` | `public.raw_routes` | ~67,000 routes |

These tables feed the staging layer (`stg_airports`, `stg_airlines`) and ultimately power the airport proximity enrichment and airline attribution in `int_flights_enriched` and the flight activity marts. Bruin handles this ingestion as `type: python` assets with `materialization: table`, meaning it recreates the tables on each pipeline run - keeping reference data fresh without any manual ETL.

### 3. Processing (Apache Flink - tumbling window jobs)

Four PyFlink jobs run inside the Flink cluster (JobManager + TaskManager containers):

| Job | Input Topic(s) | Output Table | Window |
|---|---|---|---|
| `flight_tumbling.py` | `flight-feeds` | `flights_tumbling` | 5 min PROCTIME |
| `seismic_tumbling.py` | `earthquake-feeds` | `seismics_tumbling` | 5 min event time |
| `weather_tumbling.py` | `weather-feeds` | `weather_tumbling` | 5 min event time |
| `flight_context_tumbling.py` | all three topics | `flight_context` | 5 min PROCTIME |

`flight_context_tumbling.py` is the core cross-stream job. It joins flights, seismic events, and weather readings on a shared 10-degree lat/lon grid cell - a deliberately coarse spatial key that avoids a full cross-join while preserving geographic relevance. Watermarks are set at 30s for seismic (low-latency USGS feed) and 60s for weather (15-min update cycle).

All jobs write to Supabase via the Flink JDBC connector (`flink-connector-jdbc-postgres`) and enable checkpointing at 10s intervals.

### 4. Serving (Bruin pipeline - `staging` → `intermediate` → `mart`)

The Bruin pipeline (`pipeline.yml`, scheduled every 2 minutes) transforms landing tables into three analytical layers:

**Staging** - clean, typed, geo-enriched views of each raw table. Key transformations include: geospatial `geography` column creation via PostGIS (`ST_MakePoint`), continent classification, 10-degree grid cell assignment, WMO weather code labels, wind severity bands, and magnitude classification for seismic events.

**Intermediate** - pre-joined, performance-optimized tables. `int_airport_grid` builds a spatial lookup grid from `stg_airports`. `int_flights_enriched` joins live flights with the nearest airport (via grid cell) and infers flight phase (`on_ground`, `takeoff_landing`, `climbing_descending`, `cruising`) and airline from country of origin.

**Marts** - four analytics-ready tables:

- `mart.mart_flight_activity` - global flight counts by country and continent, enriched with airline metadata, flight phase breakdown, and a 2-hour trend window.
- `mart.mart_weather_conditions` - regional weather aggregates with condition summaries, wind alerts, and temperature trends from tumbling windows.
- `mart.mart_seismic_activity` - seismic statistics by region over 24h, including magnitude class distribution, risk level classification, and hourly event trends.
- `mart.mart_flight_context` - the cross-stream enrichment mart. One row per active 10-degree grid cell, combining Flink window aggregates (flights + seismic + weather) with live flight phase data and a composite **risk score (0–100)** calculated from airborne density, seismic magnitude, wind severity, and visibility.

---

## Data Warehouse Design & Optimization

### Why Supabase (PostgreSQL) instead of BigQuery or Snowflake

SkyPulse is a **real-time upsert-heavy pipeline**, which makes columnar cloud DWHs a poor fit for the landing layer:

- **BigQuery Streaming Inserts** cost $0.01 per 200 MB and do not support `ON CONFLICT ... DO UPDATE` — meaning every flight state update (every 90s, ~9,000 rows) would require a full table scan + merge job or accumulate duplicates. Running this pipeline for a month would generate tens of thousands of streaming insert API calls.
- **Snowflake and Redshift** have similar constraints: micro-batch upserts require `MERGE` statements that lock tables and are expensive at high cadence.
- **Supabase (PostgreSQL)** supports native `INSERT ... ON CONFLICT DO UPDATE` (upsert) with row-level granularity, no per-row cost, and PostGIS for geospatial operations — all essential for a pipeline that continuously updates the latest known state of ~9,000 live aircraft.

The analytical (mart) layer is append-light and read-heavy, which is where columnar optimization matters — and where the indexing strategy below applies.

### Partitioning & Clustering Equivalent in PostgreSQL

PostgreSQL does not use BigQuery-style partition columns or clustering keys, but achieves equivalent query performance through **partial indexes**, **expression indexes**, **GiST indexes**, and **composite primary keys** that serve as implicit clustering mechanisms. Each optimization is documented below with the upstream query it targets.

#### Landing tables — primary key as clustering key

| Table | PK / Unique Key | Query pattern optimized |
|---|---|---|
| `public.flights` | `icao24` | Upsert by transponder ID; point lookup by aircraft |
| `public.weather` | `(latitude, longitude)` | Upsert by grid point; range scan by region |
| `public.seismics` | `id` (serial) | Append-only; range scan by `event_time` |
| `public.flights_tumbling` | `(window_start, window_end, origin_country)` | Window range + country filter |
| `public.seismics_tumbling` | `(window_start, window_end, region)` | Window range + region filter |
| `public.weather_tumbling` | `(window_start, window_end, region_name)` | Window range + region filter |
| `public.flight_context` | `(window_start, window_end, grid_lat, grid_lon)` | Window range + spatial grid filter |

Composite PKs on tumbling window tables cluster data by time window first, then by geographic key — matching the dominant query pattern in the mart layer (filter by recent window, group by region/country).

#### Geospatial index — `idx_stg_airports_geog`

```sql
CREATE INDEX idx_stg_airports_geog ON staging.stg_airports USING gist (geog);
```

Used by: `int_flights_enriched` — nearest-airport lookup via `ST_DWithin`. Without this GiST index, the query degrades to a sequential scan over ~7,700 airports for every live flight, producing a cross-join-like explosion. With the index, PostGIS prunes the search space to a bounding box before computing geodesic distance, keeping the join O(log n).

#### Grid cell lookup index — `idx_airport_grid_lookup`

```sql
CREATE INDEX idx_airport_grid_lookup ON intermediate.int_airport_grid (grid_lat, grid_lon);
```

Used by: `int_flights_enriched` and `mart_flight_context` — joins on the 10-degree spatial grid key. The grid is the shared geographic key across all three streams (flights, seismic, weather). This index makes each grid cell lookup O(1) instead of a full scan of the ~500-row airport grid table on every join.

#### Expression index — `idx_stg_flights_grid_computed`

```sql
CREATE INDEX idx_stg_flights_grid_computed
    ON staging.stg_flights ((FLOOR(latitude * 2) / 2), (FLOOR(longitude * 2) / 2));
```

Used by: `int_flights_enriched` — the join key is the computed expression `FLOOR(lat*2)/2`, not a stored column. Without an expression index, PostgreSQL cannot use any index for this join and falls back to a sequential scan over all staged flights. This index is the exact equivalent of a BigQuery clustering key on a derived geographic bucket column.

#### Partial index — `idx_stg_airlines_country`

```sql
CREATE INDEX idx_stg_airlines_country ON staging.stg_airlines (country) WHERE is_active = TRUE;
```

Used by: `int_flights_enriched` and `mart_flight_activity` — country-to-airline attribution join. The `WHERE is_active = TRUE` predicate filters out ~40% of the airline table (inactive carriers) at index build time, making the index smaller and faster than a full B-tree. This is the PostgreSQL equivalent of a BigQuery clustered column with a pushed-down filter.

#### Additional lookup indexes

```sql
CREATE INDEX idx_stg_airports_iata ON staging.stg_airports (iata_code);
CREATE INDEX idx_stg_airports_icao ON staging.stg_airports (icao_code);
CREATE INDEX idx_stg_airlines_iata ON staging.stg_airlines (iata_code);
CREATE INDEX idx_stg_airlines_icao ON staging.stg_airlines (icao_code);
```

Used by: mart joins that resolve IATA/ICAO codes for display in the dashboard. Point lookups on these columns without indexes would require sequential scans over ~7,700 airports and ~5,800 airlines on every mart refresh.

---

## Database Entity Relationship

The diagram below shows the key tables across all layers, their primary keys, and how they relate through shared geographic identifiers (`grid_lat`, `grid_lon`) and reference joins.

```mermaid
erDiagram

    %% ── Public (Landing) ──────────────────────────────────────────
    public_flights {
        TEXT icao24 PK
        TEXT callsign
        TEXT origin_country
        TIMESTAMP time_position
        TIMESTAMP last_contact
        FLOAT longitude
        FLOAT latitude
        FLOAT baro_altitude
        BOOLEAN on_ground
        FLOAT velocity
        FLOAT true_track
        INT category
    }

    public_weather {
        FLOAT latitude PK
        FLOAT longitude PK
        TEXT region_name
        FLOAT elevation_m
        INT weathercode
        FLOAT windspeed_ms
        FLOAT temperature_c
        FLOAT precipitation_mm
        FLOAT visibility_m
        TIMESTAMP snapshot_ts
    }

    public_seismics {
        SERIAL id PK
        FLOAT mag
        TEXT place
        INT tsunami
        TIMESTAMP event_time
        TEXT event_type
        FLOAT lat
        FLOAT lon
        FLOAT depth
    }

    public_raw_airports {
        INT airport_id PK
        TEXT name
        TEXT city
        TEXT country
        TEXT iata
        TEXT icao
        FLOAT latitude
        FLOAT longitude
        INT altitude_ft
        TEXT tz_database
    }

    public_raw_airlines {
        INT airline_id PK
        TEXT name
        TEXT iata
        TEXT icao
        TEXT country
        BOOLEAN active
    }

    %% ── Staging ────────────────────────────────────────────────────
    staging_stg_flights {
        TEXT icao24 PK
        FLOAT longitude
        FLOAT latitude
        FLOAT baro_altitude_m
        FLOAT velocity_knots
        TEXT continent
        INT grid_lat
        INT grid_lon
        GEOGRAPHY geog
    }

    staging_stg_weather {
        FLOAT latitude PK
        FLOAT longitude PK
        TEXT region_name
        FLOAT temperature_c
        FLOAT windgusts_ms
        TEXT weather_label
        TEXT wind_severity
        INT grid_lat
        INT grid_lon
    }

    staging_stg_seismics {
        INT id PK
        FLOAT mag
        TEXT region
        FLOAT depth_km
        TEXT magnitude_class
        BOOLEAN is_tsunami_related
        INT grid_lat
        INT grid_lon
    }

    staging_stg_airports {
        INT airport_id PK
        TEXT airport_name
        TEXT iata_code
        TEXT icao_code
        TEXT city
        TEXT country
        TEXT continent
        FLOAT latitude
        FLOAT longitude
        FLOAT altitude_m
        TEXT tz_database
        GEOGRAPHY geog
    }

    staging_stg_airlines {
        INT airline_id PK
        TEXT airline_name
        TEXT iata_code
        TEXT icao_code
        TEXT country
        BOOLEAN is_active
    }

    staging_stg_flights_tumbling {
        TIMESTAMP window_start PK
        TIMESTAMP window_end PK
        TEXT origin_country PK
        BIGINT flight_count
        BIGINT airborne_count
        FLOAT avg_altitude_m
        FLOAT avg_velocity_ms
    }

    staging_stg_seismics_tumbling {
        TIMESTAMP window_start PK
        TIMESTAMP window_end PK
        TEXT region PK
        FLOAT avg_magnitude
        FLOAT max_magnitude
        BIGINT event_count
        BIGINT tsunami_count
        FLOAT avg_depth
    }

    staging_stg_weather_tumbling {
        TIMESTAMP window_start PK
        TIMESTAMP window_end PK
        TEXT region_name PK
        FLOAT avg_temperature_c
        FLOAT avg_windspeed_ms
        FLOAT avg_visibility_m
        FLOAT total_precip_mm
        BIGINT snapshot_count
    }

    staging_stg_flight_context {
        TIMESTAMP window_start PK
        TIMESTAMP window_end PK
        INT grid_lat PK
        INT grid_lon PK
        BIGINT flight_count
        BIGINT airborne_count
        INT nearby_eq_count
        FLOAT max_eq_magnitude
        INT tsunami_count
        FLOAT avg_temperature_c
        FLOAT avg_windgusts_ms
        FLOAT avg_visibility_m
    }

    %% ── Intermediate ───────────────────────────────────────────────
    intermediate_int_airport_grid {
        FLOAT grid_lat PK
        FLOAT grid_lon PK
        TEXT airport_name
        TEXT iata_code
        TEXT icao_code
        TEXT city
        TEXT country
        TEXT continent
        TEXT tz_database
        FLOAT altitude_m
    }

    intermediate_int_flights_enriched {
        TEXT icao24 PK
        FLOAT grid_lat
        FLOAT grid_lon
        FLOAT baro_altitude_m
        FLOAT velocity_knots
        TEXT flight_phase
        TEXT current_continent
        TEXT nearest_airport_name
        TEXT nearest_airport_iata
        TEXT nearest_airport_city
        BOOLEAN near_airport
        TEXT airline_name
        TEXT airline_iata
    }

    %% ── Marts ──────────────────────────────────────────────────────
    mart_flight_activity {
        TEXT origin_country PK
        TEXT continent
        TEXT airline_name
        BIGINT live_flight_count
        BIGINT live_airborne_count
        BIGINT cruising_count
        FLOAT avg_speed_knots
        FLOAT avg_altitude_m
        TEXT most_common_airport
        FLOAT peak_flights_2h
        TIMESTAMP refreshed_at
    }

    mart_weather_conditions {
        TEXT region_name PK
        INT station_count
        FLOAT avg_temp_c
        FLOAT max_windgusts_ms
        FLOAT avg_visibility_m
        FLOAT total_precip_mm
        TEXT dominant_weather
        TEXT condition_summary
        TIMESTAMP refreshed_at
    }

    mart_seismic_activity {
        TEXT region PK
        INT event_count_24h
        FLOAT max_magnitude
        FLOAT avg_magnitude
        FLOAT avg_depth_km
        INT tsunami_events
        TEXT risk_level
        TIMESTAMP refreshed_at
    }

    mart_flight_context {
        TIMESTAMP window_start PK
        TIMESTAMP window_end PK
        INT grid_lat PK
        INT grid_lon PK
        TEXT continent
        TEXT primary_airport_name
        BIGINT flight_count
        BIGINT airborne_count
        INT nearby_eq_count
        FLOAT max_eq_magnitude
        TEXT wind_severity
        FLOAT avg_temperature_c
        FLOAT risk_score
        TIMESTAMP refreshed_at
    }

    %% ── Relationships ──────────────────────────────────────────────
    public_flights             ||--o{ staging_stg_flights             : "raw → staged"
    public_weather             ||--o{ staging_stg_weather             : "raw → staged"
    public_seismics            ||--o{ staging_stg_seismics            : "raw → staged"
    public_raw_airports        ||--o{ staging_stg_airports            : "raw → staged"
    public_raw_airlines        ||--o{ staging_stg_airlines            : "raw → staged"

    staging_stg_airports       ||--o{ intermediate_int_airport_grid   : "grid lookup"
    staging_stg_flights        ||--o{ intermediate_int_flights_enriched : "flight base"
    intermediate_int_airport_grid ||--o{ intermediate_int_flights_enriched : "grid_lat/grid_lon join"
    staging_stg_airlines       ||--o{ intermediate_int_flights_enriched : "country join"

    staging_stg_flights_tumbling  ||--o{ mart_flight_activity         : "tumbling window"
    intermediate_int_flights_enriched ||--o{ mart_flight_activity     : "live enrichment"
    staging_stg_airlines          ||--o{ mart_flight_activity         : "airline metadata"

    staging_stg_seismics          ||--o{ mart_seismic_activity        : "24h events"
    staging_stg_seismics_tumbling ||--o{ mart_seismic_activity        : "window stats"

    staging_stg_weather           ||--o{ mart_weather_conditions      : "regional agg"
    staging_stg_weather_tumbling  ||--o{ mart_weather_conditions      : "window trend"

    staging_stg_flight_context    ||--o{ mart_flight_context          : "flink window"
    intermediate_int_flights_enriched ||--o{ mart_flight_context      : "live flights grid"
    staging_stg_weather           ||--o{ mart_flight_context          : "weather overlay"
    staging_stg_seismics          ||--o{ mart_flight_context          : "24h seismic"
```

---

## Project Tree

```text
SkyPulse-Streaming-Pipeline/
├── .github/
│   └── workflows/
│       └── ci.yml                        # Lint job (Ruff + pre-commit)
├── .pre-commit-config.yaml
├── .python-version                       # 3.12
├── Makefile                              # Full lifecycle automation
├── pyproject.toml                        # uv project + Ruff config
├── uv.lock
│
├── infra/
│   ├── .gitignore                        # Excludes .tfvars, .terraform/, state files
│   ├── setup.bat                         # One-command provisioning (init → apply → migrate)
│   ├── migrations/
│   │   └── migrate.py                    # DDL + RLS + Realtime + .env patcher
│   └── terraform/
│       ├── .terraform.lock.hcl
│       ├── provider.tf                   # supabase/supabase provider ~> 1.0
│       ├── project.tf                    # supabase_project resource
│       ├── settings.tf                   # PostgREST API schema exposure
│       ├── variables.tf                  # access_token, org_id, region, db_password
│       └── outputs.tf                    # project_ref, project_url (consumed by migrate.py)
│
├── deploy/
│   ├── docker-compose.yml                # Redpanda, Postgres, Flink JobManager + TaskManager
│   ├── Dockerfile.flink                  # PyFlink 2.2 + JDBC/Kafka connectors
│   ├── flink-config.yaml                 # Flink cluster configuration
│   └── pyproject.flink.toml             # Flink-specific Python dependencies
│
├── src/
│   ├── logger.py                         # Shared structured logger
│   ├── models/
│   │   ├── flight.py                     # Flight dataclass + serializer/deserializer
│   │   ├── seismic.py                    # Earthquake dataclass + serializer/deserializer
│   │   └── weather.py                   # Weather dataclass + serializer/deserializer
│   ├── producers/
│   │   ├── flight_producer.py            # OpenSky Network → flight-feeds
│   │   ├── seismic_producer.py           # USGS GeoJSON → earthquake-feeds
│   │   ├── weather_producer.py           # Open-Meteo grid → weather-feeds
│   │   └── misc/
│   │       └── cache.sqlite              # requests-cache for Open-Meteo
│   ├── consumers/
│   │   ├── flight_consumer.py            # flight-feeds → public.flights
│   │   ├── seismic_consumer.py           # earthquake-feeds → public.seismics
│   │   └── weather_consumer.py          # weather-feeds → public.weather
│   └── jobs/
│       ├── flight_tumbling.py            # Flink: 5-min flight aggregation
│       ├── seismic_tumbling.py           # Flink: 5-min seismic aggregation
│       ├── weather_tumbling.py           # Flink: 5-min weather aggregation
│       └── flight_context_tumbling.py   # Flink: cross-stream join + grid output
│
├── pipeline/
│   ├── pipeline.yml                      # Bruin pipeline definition (every 2 min)
│   └── assets/
│       ├── ingestion/
│       │   ├── ingest_airlines.py        # OpenFlights → public.raw_airlines
│       │   ├── ingest_airports.py        # OpenFlights → public.raw_airports
│       │   ├── ingest_planes.py          # OpenFlights → public.raw_planes
│       │   ├── ingest_routes.py          # OpenFlights → public.raw_routes
│       │   └── requirements.txt
│       ├── staging/
│       │   ├── stg_flights.sql
│       │   ├── stg_weather.sql
│       │   ├── stg_seismics.sql
│       │   ├── stg_airports.sql
│       │   ├── stg_airlines.sql
│       │   ├── stg_routes.sql
│       │   ├── stg_flights_tumbling.sql
│       │   ├── stg_seismics_tumbling.sql
│       │   └── stg_weather_tumbling.sql
│       ├── intermediate/
│       │   ├── int_airport_grid.sql
│       │   └── int_flights_enriched.sql
│       └── marts/
│           ├── mart_flight_activity.sql
│           ├── mart_weather_conditions.sql
│           ├── mart_seismic_activity.sql
│           └── mart_flight_context.sql
│
├── scripts/
│   ├── bruin/
│   │   ├── run_bruin.bat
│   │   ├── test_bruin.bat
│   │   └── validate_bruin.bat
│   ├── tree.py
│   └── wait_topics.py                   # Polls Redpanda until topics are ready
│
└── notebooks/
    ├── flights_producer.ipynb
    ├── flights_consumer.ipynb
    ├── seismic_producer.ipynb
    ├── seismic_consumer.ipynb
    ├── weather_producer.ipynb
    └── weather_consumer.ipynb
```

---

## Getting Started

### Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Python | 3.12 | Managed via `.python-version` |
| `uv` | latest | [Install](https://docs.astral.sh/uv/getting-started/installation/) |
| `make` | latest | See note below |
| Docker + Docker Compose | latest | Required for Redpanda and Flink |
| Terraform | >= 1.6 | [Install](https://developer.hashicorp.com/terraform/install) — required for `make infra-deploy` |
| Bruin CLI | latest | [Install](https://bruin-data.github.io/bruin/getting-started/introduction.html) |
| Supabase account | - | Free tier is sufficient — access token needed for Terraform |

> **Installing `make`**
>
> - **Linux (Debian/Ubuntu):** `sudo apt install make`
> - **macOS:** included with Xcode Command Line Tools - run `xcode-select --install`, or via Homebrew: `brew install make`
> - **Windows:** install via [Chocolatey](https://chocolatey.org/) with `choco install make`, or use [GnuWin32](https://gnuwin32.sourceforge.net/packages/make.htm). WSL2 is also a clean alternative - `make` works out of the box inside the Linux subsystem.

### Environment Setup

Copy and populate the `.env` file at the project root:

```bash
cp .env.example .env
```

Required variables:

```dotenv
# Redpanda
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Redpanda topic names
TOPIC_FLIGHTS=flight-feeds
TOPIC_SEISMIC=earthquake-feeds
TOPIC_WEATHER=weather-feeds

# Supabase (PostgreSQL) — auto-populated by make infra-deploy
SUPABASE_HOST=<your-supabase-host>
SUPABASE_PORT=5432
SUPABASE_USER=postgres
SUPABASE_PASSWORD=<your-password>
SUPABASE_DATABASE=postgres

# OpenSky Network (OAuth2)
OPENSKY_CLIENT_ID=<your-client-id>
OPENSKY_CLIENT_SECRET=<your-client-secret>
```

> **Note:** the five `SUPABASE_*` variables are automatically written to `.env` by `migrate.py` at the end of `make infra-deploy`. You only need to fill them in manually if you skip the Terraform provisioning step and point to a pre-existing Supabase project.

Install Python dependencies:

```bash
make install
```

### Provision the Supabase Infrastructure

SkyPulse uses Terraform to provision the Supabase project and a Python migration runner to initialize the full schema. A single command handles everything:

```bash
make infra-deploy
```

This runs `infra/setup.bat`, which executes the following steps in order:

1. `terraform init` → downloads the `supabase/supabase` provider
2. `terraform validate` → checks configuration syntax
3. `terraform plan` → previews the resources to be created
4. `terraform apply -auto-approve` → provisions the Supabase project in the target region and configures the PostgREST API to expose all four schemas (`public`, `staging`, `intermediate`, `mart`)
5. `uv run python infra/migrations/migrate.py` → reads Terraform outputs and runs the full migration sequence (see below)

**What the migration runner does (`infra/migrations/migrate.py`):**

| Step | What it does |
|---|---|
| **Wait for DB** | Polls the database up to 10 times (15s apart) until it's reachable — Supabase projects take ~1 min to boot after `apply` |
| **DDL migrations** | Enables PostGIS, creates `staging`/`intermediate`/`mart` schemas, grants `USAGE` to PostgREST roles, and creates all landing tables, tumbling window tables, and performance indexes |
| **Row Level Security** | Enables RLS on all 26 tables across all schemas; creates `service_role` (full access) and `anon`/`authenticated` (read-only) policies |
| **Supabase Realtime** | Adds all public tables to the `supabase_realtime` publication |
| **Patch `.env`** | Writes `SUPABASE_HOST`, `SUPABASE_USER`, `SUPABASE_PASSWORD`, etc. back to the root `.env` automatically |

After `make infra-deploy` completes, the database is fully wired and the `.env` is ready — no manual SQL or copy-pasting of connection strings required.

**Terraform variables** — create `infra/terraform/terraform.tfvars` (gitignored):

```hcl
supabase_access_token = "<your-supabase-access-token>"
org_id                = "<your-supabase-org-slug>"
supabase_region       = "us-east-1"
db_password           = "<a-strong-password>"
```

Your Supabase access token is available at [supabase.com/dashboard/account/tokens](https://supabase.com/dashboard/account/tokens). The org slug is visible in the URL of your Supabase dashboard.

> **Performance indexes.** The migration runner also creates all recommended indexes as part of the initial setup. A second pass after the first Bruin run will create indexes on staging/intermediate tables that don't exist yet at migration time — re-running `migrate.py` is safe, as all statements use `IF NOT EXISTS`. The indexes with the highest impact are:
>
> - `idx_stg_airports_geog` — GiST index on `staging.stg_airports(geog)` for PostGIS proximity joins
> - `idx_airport_grid_lookup` — B-tree on `int_airport_grid(grid_lat, grid_lon)` for O(1) grid cell lookup
> - `idx_stg_flights_grid_computed` — expression index on `FLOOR(latitude*2)/2, FLOOR(longitude*2)/2` matching the exact join key in `int_flights_enriched`
> - `idx_stg_airlines_country` — partial index on active airlines only, used in the country → airline attribution join

To tear down the Supabase project completely:

```bash
make infra-destroy
```

### Start the Infrastructure

First, provision and initialize Supabase via Terraform:

```bash
make infra-deploy
```

This creates the Supabase project, runs all migrations, configures RLS and Realtime, and patches the `.env` automatically. See [Provision the Supabase Infrastructure](#provision-the-supabase-infrastructure) above for the full breakdown.

Then build and start Redpanda and the Flink cluster locally:

```bash
make deploy
```

This brings up three containers: `redpanda`, `jobmanager` (Flink UI at `http://localhost:8081`), and `taskmanager`.

Create Redpanda topics (or reset existing ones):

```bash
make clean-topics
```

### Run the Streaming Pipeline

Start all producers, consumers, and Flink jobs in one command:

```bash
make streaming
```

This executes the following steps in sequence:

1. `make producers` - launches `seismic_producer.py`, `flight_producer.py`, and `weather_producer.py` in separate terminal windows
2. `make consumers` - launches `seismic_consumer.py`, `flight_consumer.py`, and `weather_consumer.py` in separate terminal windows
3. `make wait-topics` - polls Redpanda until all three topics have received at least one message
4. `make jobs` - submits all four Flink jobs to the JobManager via `docker exec`

To run components individually:

```bash
make producers     # producers only
make consumers     # consumers only
make jobs          # Flink jobs only (requires topics to have data)
```

### Run the Bruin Transformation Pipeline

Bruin orchestrates two distinct responsibilities in this project:

**1. Static reference ingestion** - on first run (or whenever reference data needs refreshing), Bruin fetches OpenFlights data and materializes it into `public.raw_airports`, `public.raw_airlines`, `public.raw_planes`, and `public.raw_routes`. This only needs to run once before the streaming pipeline starts, since this data changes infrequently.

**2. Layered SQL transformations** - scheduled every 2 minutes (`pipeline.yml`), Bruin processes the streaming landing tables through the full `staging → intermediate → mart` model, producing the analytics-ready outputs consumed by downstream dashboards or queries.

Run the full pipeline (both ingestion and transformations):

```bash
make run-pipeline
```

Bruin resolves and executes assets in dependency order: `ingestion` → `staging` → `intermediate` → `marts`. On subsequent runs, the static ingestion assets re-materialize their tables (truncate + reload), while the SQL transformation assets read from the latest streaming data in the landing tables.

> **Tip:** run the static ingestion assets once before starting the streaming pipeline. `int_flights_enriched` and `mart_flight_activity` depend on `staging.stg_airports` and `staging.stg_airlines` being populated to perform airport proximity lookups and airline attribution.

### Teardown

```bash
make deploy-destroy    # Stop and remove Flink + Redpanda containers
make postgres-destroy  # Stop and remove the local Postgres container (if used)
make infra-destroy     # Destroy the Supabase project via Terraform (irreversible)
```

---

## License

Distributed under the MIT License. See `LICENSE` for details.
