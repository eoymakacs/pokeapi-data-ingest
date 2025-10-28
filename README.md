# PokeAPI Challenge

## Overview
This project is designed to ingest and analyze Pokemon data. The goal is to ingest, query, and manipulate Pokemon data, including their types and stats, in a relational database. 

---

## Database Schema

This project uses a simple SQLite (or compatible) database with the following tables:

### `pokemon`
Stores basic Pokémon information:

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Primary key, unique Pokemon ID |
| `name` | TEXT | Name of the Pokemon |
| `types` | TEXT | Pokemon type(s), separated by `/` if multiple |

SQL Definition:
```sql
CREATE TABLE IF NOT EXISTS pokemon (
    id INTEGER PRIMARY KEY,
    name TEXT,
    types TEXT
);
```


### `pokemon_stats`
Stores Pokemon stat data:

| Column | Type | Description |
|--------|------|-------------|
| `pokemon_id` | INTEGER | Foreign key referencing pokemon.id |
| `stat_name` | TEXT | stat name |
| `value` | TEXT | base_stat value |

SQL Definition:
```sql
CREATE TABLE IF NOT EXISTS pokemon_stats (
    pokemon_id INTEGER,
    stat_name TEXT,
    value INTEGER,
    PRIMARY KEY (pokemon_id, stat_name),
    FOREIGN KEY (pokemon_id) REFERENCES pokemon(id)
);
```

## How to Run

1. **Install dependencies**
```bash
pip install -r requirements.txt
```
2. **Run Data Ingestion Process**
```bash
python data_ingest.py
```
3. **Run Anaysis Scripts (Answers for Queries)**
```bash
python run_analytics.py
```

## Logging

Logs are printed both to the console and to a separate log file for easy debugging and monitoring.

## Ingestion Mode: Incremental vs Full

This project can run in two modes: **Full Refresh** or **Incremental**. The mode is controlled by the global variable `is_incremental`.

### Behavior

- **Full Refresh (`is_incremental = False`)**
  - The process assumes that it is running from scratch.
  - All existing tables (`pokemon`, `pokemon_stats`) are **dropped and recreated**.
  - All data is fetched and stored anew from the PokeAPI.

- **Incremental (`is_incremental = True`)**
  - The process assumes that it is updating existing data.
  - Existing tables are preserved.
  - The script **checks for deleted stats** for Pokemon and updates the tables accordingly.
  - Only new or changed data is fetched and processed.
