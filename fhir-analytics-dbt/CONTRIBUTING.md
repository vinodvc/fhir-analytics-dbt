# Contributing

## dbt Model Conventions

### Naming
| Layer | Prefix | Example |
|---|---|---|
| Staging | `stg_fhir__` | `stg_fhir__patients` |
| Intermediate | `int_` | `int_patient_spine` |
| Marts | `mart_` | `mart_care_gaps` |

### Style
- All SQL lowercase (keywords included)
- CTE-first pattern — no subqueries in `from` clauses
- Every staging model gets a `schema.yml` entry with `description` + tests on all PKs
- `_ingested_at` and `_dbt_updated_at` meta columns on every model

### Materialization defaults
- `staging/` → **view** (cheap, always fresh)
- `intermediate/` → **table** (joined, rebuilt daily)
- `marts/` → **table** (queried heavily by dashboards)

### Testing requirements (per model)
- `unique` + `not_null` on all primary keys
- `relationships` test on all FK joins to staging models
- `accepted_values` on all status / category fields
- At least one `dbt_expectations` range test on any numeric KPI column

## Python Style
- Type hints on all function signatures
- `logging` (not `print`) in production scripts
- `--dry-run` flag on all ingestion/write scripts
- `ruff` for linting, `mypy` for type checking

## Adding a New FHIR Resource Type

1. Add a flattener function to `scripts/ingest_metriport_fhir.py`
2. Add a `stg_fhir__<resource>.sql` staging model
3. Add `schema.yml` entries with tests
4. Update `int_patient_spine.sql` if the resource has patient-level aggregations
5. Add the resource to `RESOURCE_TYPES` in the ingestion script

## Running Locally (no Snowflake)

```bash
# Generate synthetic data
python scripts/care_gap_model.py --source synthetic --output /tmp/predictions/

# Run dbt against DuckDB (no Snowflake needed)
pip install dbt-duckdb
dbt run --target dev_duckdb
```
