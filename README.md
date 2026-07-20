 week4_etl#_pipeline

A production-grade, idempotent ETL pipeline with a built-in data quality gate,
built for the Week 4 assignment. The pipeline is domain-agnostic; it ships
configured for an **industrial IoT sensor readings** dataset (temperature,
pressure, humidity per sensor per timestamp), which stands in for the
project's source dataset.

## What this pipeline does

```
data/source/raw_sensor_data.csv
        │
        ▼
   EXTRACT  ───►  VALIDATE (quality gate)  ───►  TRANSFORM  ───►  LOAD
        │                  │                                       │
        │                  └─ halts pipeline if rules fail          │
        ▼                                                           ▼
   pipeline.log  ◄──────────── logs every stage ────────────►  data/processed/warehouse.db
```

1. **Extract** — reads the raw CSV from `SOURCE_DATA_PATH`.
2. **Validate** — runs a 6-rule data quality suite (see below). If any rule
   fails, the pipeline logs the full report and **halts before loading**
   (exit code `1`), so bad data never reaches the warehouse.
3. **Transform** — drops exact duplicates and unusable rows, parses
   timestamps, adds a derived `temperature_f` column and an
   `is_out_of_range` flag.
4. **Load** — writes to a SQLite table, **idempotently**: the target table
   is dropped and recreated on every run, and `reading_id` is enforced as a
   unique index. Re-running the pipeline on the same file never produces
   duplicate rows.

Every run appends a timestamped entry to `pipeline.log` recording start time,
end time, row counts at each stage, the full validation report, and any
errors (with full tracebacks for unexpected crashes).

## 1. Install dependencies

```bash
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

`requirements.txt` includes `great_expectations`. The pipeline's own quality
gate (`validation.py`) does **not** require the package to be installed — it
reads `great_expectations/expectations/sensor_data_suite.json` directly with
pandas — but the same JSON file is a valid GE expectation suite, so you can
load it into a real GE `FileDataContext` if you want the full GE tooling
(Data Docs, etc.). See the comment at the top of `validation.py`.

## 2. Configure your environment

```bash
cp .env.example .env
```

Then edit `.env`:

| Variable | Purpose |
|---|---|
| `SOURCE_DATA_PATH` | Path to the input CSV |
| `TARGET_DB_PATH` | Path to the SQLite output database |
| `TARGET_TABLE` | Table name to load into |
| `GE_SUITE_PATH` | Path to the expectation suite JSON |
| `LOG_FILE` / `LOG_LEVEL` | Logging destination and verbosity |
| `MIN/MAX_TEMPERATURE_C`, `MIN/MAX_PRESSURE_KPA`, `MIN/MAX_HUMIDITY_PCT` | Quality thresholds referenced by the transform step |
| `HALT_ON_VALIDATION_FAILURE` | `true` (default) stops the pipeline on any failed rule; `false` logs a warning and continues |

**Never commit your real `.env` file** — it's already listed in `.gitignore`.
Only `.env.example` (with placeholder/non-secret defaults) is committed.

## 3. Run the pipeline

```bash
python run_pipeline.py
```

The repo ships with `data/source/raw_sensor_data.csv`, which has a handful of
intentionally dirty rows (an out-of-range temperature, a negative pressure, a
null sensor ID, an out-of-range humidity value, and a duplicate reading ID).
Running the pipeline against it as-is will **halt at the validation step**
and exit with code `1` — this is expected, and demonstrates the quality gate
working. A cleaned version, `data/source/raw_sensor_data_clean.csv`, is also
included; point `SOURCE_DATA_PATH` at it to see a full successful run that
loads 500 rows into `data/processed/warehouse.db`.

Check the results:

```bash
tail -n 30 pipeline.log
sqlite3 data/processed/warehouse.db "SELECT COUNT(*) FROM sensor_readings;"
```

## 4. Data quality rules (`great_expectations/expectations/sensor_data_suite.json`)

| # | Rule | Column |
|---|---|---|
| 1 | Not null | `reading_id` |
| 2 | Unique (no duplicate IDs) | `reading_id` |
| 3 | Not null | `sensor_id` |
| 4 | Between -40 and 100 | `temperature_c` |
| 5 | Strictly > 0 and ≤ 300 | `pressure_kpa` |
| 6 | Between 0 and 100 | `humidity_pct` |

## 5. Automation

See `cron_automation/cron_job_snippet.txt` for a verified Linux `crontab`
entry and a Windows `schtasks`/Task Scheduler equivalent that run
`run_pipeline.py` on a daily schedule.

## Project structure

```
week4_etl_pipeline/
├── run_pipeline.py                 # extract/validate/transform/load + logging
├── validation.py                   # GE-style expectation suite interpreter
├── requirements.txt
├── .env.example
├── .gitignore
├── README.md
├── hackathon_reflection.md
├── Week4_Technical_Brief_Stanley_Metone.pdf
├── data/
│   ├── source/
│   │   ├── raw_sensor_data.csv         # dirty sample (triggers the quality gate)
│   │   └── raw_sensor_data_clean.csv   # clean sample (successful run)
│   └── processed/
│       └── warehouse.db                # generated on run
├── great_expectations/
│   ├── great_expectations.yml
│   └── expectations/
│       └── sensor_data_suite.json
└── cron_automation/
    └── cron_job_snippet.txt
```
