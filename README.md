# Wind Turbine Data Pipeline — Technical Take-Home

A small PySpark proof-of-concept that ingests wind-turbine CSV measurements, cleans sensor data, calculates 24-hour turbine statistics, identifies anomalous turbines, and persists the processed outputs as Spark SQL tables.

## Design

The pipeline is intentionally small and modular because the exercise asks for a proof-of-concept with emphasis on code clarity, testability, and the required functionality rather than application architecture.

Flow:

```text
CSV files
   ↓
Explicit-schema ingestion
   ↓
Data cleaning
   - remove duplicate turbine/timestamp records
   - reject rows without timestamp/turbine_id
   - mark physically invalid values as missing
   - detect wind-speed/power-output outliers with per-turbine IQR
   - median-impute missing measurement values per turbine
   ↓
24-hour turbine summary
   - min / max / average / stddev power
   - reading count / missing-reading count
   ↓
Fleet anomaly detection
   - compare turbine daily average against fleet daily mean ± 2 stddev
   ↓
Spark SQL managed tables
```

## Assumptions

1. **One measurement is expected per turbine per hour.** Therefore a complete 24-hour period contains 24 readings. Missing whole sensor entries are reported through `missing_readings`; the pipeline does not fabricate measurements for timestamps that never arrived.
2. **Sensor outliers and turbine anomalies are different concepts.** Sensor-level numeric outliers are cleaned using a 1.5×IQR rule per turbine. Turbine anomalies are calculated separately from each turbine's 24-hour average using the assessment's ±2 standard deviation definition.
3. **Wind direction is circular.** IQR outlier detection is not applied to direction because 0° and 359° are adjacent. Values outside `[0, 360)` are treated as invalid.
4. **No turbine rated capacity is provided.** Power output below zero is treated as invalid, but an arbitrary maximum MW threshold is not imposed.
5. **Missing measurement values are median-imputed per turbine** for this POC. For production, the imputation policy should be agreed with domain experts and may use temporal interpolation or another sensor-aware method.
6. The anomaly requirement is interpreted at **turbine/day level**: each turbine's daily average is compared with the distribution of all turbine daily averages for the same day.

## Output tables

The local Spark SQL database `wind_turbine` contains:

- `cleaned_readings` — cleaned measurement-level data
- `daily_summary` — daily statistics, completeness metrics, anomaly bounds and flag
- `anomalies` — only turbine/day records flagged as anomalous

Spark managed tables were chosen so the POC remains self-contained while still storing data in queryable database tables.

## Run locally

Requirements:

- Python 3.10+
- Java 8/11/17

Create an environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
```

Run the pipeline:

```bash
python run_pipeline.py
```

Run tests:

```bash
pytest -q
```

## Scalability / productionisation

For a production version I would keep the transformation functions largely unchanged but replace the local ingestion/storage/orchestration around them:

- Land daily source files in object storage and process only new files rather than rescanning full history.
- Store raw and cleaned data in partitioned Parquet/Delta tables (for example partitioned by date).
- Use checkpointing/idempotent writes so reruns do not duplicate readings.
- Add schema/data-quality monitoring and alert when expected turbine/hour records are missing.
- Orchestrate with Airflow or a managed Spark platform such as Databricks Jobs.
- Add structured logging, metrics, retry/error handling, CI/CD and integration tests.
- Use a production warehouse/lakehouse according to analytics requirements and manage secrets through the platform's secret store.

## Notes on the supplied data

The supplied month contains 15 turbines across three CSV groups. The pipeline does not rely on the exact file count or turbine IDs; it reads all files matching `data_group_*.csv` and groups calculations by `turbine_id`.
