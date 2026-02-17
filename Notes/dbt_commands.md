# DBT Commands Reference

## taxi_rides_ny Project

---

## 1. Project Initialization

| Command | Description |
|---------|-------------|
| `dbt init` | Initialize a new dbt project in the current directory |
| `dbt deps` | Install packages and dependencies defined in packages.yml |

---

## 2. Configuration & Validation

| Command | Description |
|---------|-------------|
| `dbt debug` | Test database connection and validate dbt configuration |
| `dbt --version` or `dbt -v` | Display the installed dbt version |
| `dbt --help` or `dbt -h` | Display help information about dbt commands |

---

## 3. Compilation

| Command | Description |
|---------|-------------|
| `dbt compile` | Compile Jinja templates to SQL without executing them (validates syntax) |

---

## 4. Data Loading

| Command | Description |
|---------|-------------|
| `dbt seed` | Load CSV files from seeds/ directory into the database as tables |
| `dbt source freshness` | Check the freshness of source data tables based on loaded_at_field |

---

## 5. Model Execution

### Basic Execution

```bash
dbt run  # Execute all models (materialized tables/views) in the project
```

### Model Selection Options

| Command | Description |
|---------|-------------|
| `dbt run --select stg_green_tripdata` | Run only the stg_green_tripdata model |
| `dbt run --select int_trips_unioned` | Run only the specified model (no upstream dependencies) |
| `dbt run --select +int_trips_unioned` | Run the model and all its upstream dependencies (+ prefix) |
| `dbt run --select +int_trips_unioned+` | Run the model, all upstream dependencies, and all downstream models |
| `dbt run --select models/staging` | Run all models in the staging directory |
| `dbt run --select tag:staging` | Run all models tagged with 'staging' in their config |
| `dbt run --select state:modified --state ./state.json` | Run only models that have been modified since the last state |

### Model Exclusion Options

| Command | Description |
|---------|-------------|
| `dbt run --exclude stg_green_tripdata` | Run all models except stg_green_tripdata |

### Execution Modifiers

| Command | Description |
|---------|-------------|
| `dbt run --full-refresh` | Force rebuild all incremental models from scratch, ignoring existing data |
| `dbt run --fail-fast` | Stop execution immediately on first failure instead of continuing |

---

## 6. Testing

| Command | Description |
|---------|-------------|
| `dbt test` | Run all data tests defined in schema.yml and tests/ directory |
| `dbt test --select stg_green_tripdata` | Run tests only for a specific model |

---

## 7. Snapshots

| Command | Description |
|---------|-------------|
| `dbt snapshot` | Execute snapshot models to capture slowly changing dimensions (SCD Type 2) |

---

## 8. Build (Combined Operations)

| Command | Description |
|---------|-------------|
| `dbt build` | Combines dbt run + dbt test + dbt seed + dbt snapshot in dependency order |
| `dbt retry` | Re-run only the models/tests that failed in the last dbt build or run |

---

## 9. Environment Targeting

| Command | Description |
|---------|-------------|
| `dbt run --target prod` | Run models against the production target environment |
| `dbt build --target prod` | Build all resources in the production environment |
| `dbt test --target prod` | Run tests in the production environment |

---

## 10. Documentation

| Command | Description |
|---------|-------------|
| `dbt docs generate` | Generate documentation for models, tests, sources, and lineage graph |
| `dbt docs serve` | Serve documentation on local web server (default: http://localhost:8080) |

---

## 11. Cleanup

| Command | Description |
|---------|-------------|
| `dbt clean` | Remove compiled SQL files, logs, and artifacts from target/ and dbt_packages/ directories |

---

## Execution History

### taxi_rides_ny Project

#### Initial Setup

```bash
dbt init
dbt deps
dbt debug
```

#### Data Loading

```bash
dbt seed  # Loaded taxi_zone_lookup.csv
```

#### Model Development

```bash
dbt run  # Created staging, intermediate, and mart models
```

**Models Created:**
- **Staging**: stg_green_tripdata, stg_yellow_tripdata
- **Intermediate**: int_trips_unioned
- **Marts**: dim_zones, dim_vendors, fct_trips, monthly_revenue_per_location

#### Production Deployment

```bash
dbt build --target prod  # First attempt (had test failures with null id values)
dbt build --target prod  # Fixed and re-ran (all tests passed)
```

#### Documentation

```bash
dbt docs generate
dbt docs serve
```

---

## Quick Reference

### Selection Syntax

- `model_name` - Run only the specified model
- `+model_name` - Run the model and all upstream (parent) models
- `model_name+` - Run the model and all downstream (child) models
- `+model_name+` - Run the model, all upstream, and all downstream models

### Common Workflows

**Development Workflow:**
```bash
dbt run --select +my_model  # Build model and dependencies
dbt test --select my_model  # Test the model
```

**Production Deployment:**
```bash
dbt build --target prod --full-refresh  # Full rebuild in production
```

**Incremental Development:**
```bash
dbt run --select state:modified  # Run only changed models
dbt test --select state:modified  # Test only changed models
```

**Debugging:**
```bash
dbt compile --select my_model  # Check compiled SQL
dbt run --select my_model --fail-fast  # Stop on first error
```
