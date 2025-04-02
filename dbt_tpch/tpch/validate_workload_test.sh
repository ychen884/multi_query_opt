# workload test

bash ./cleanup.sh

echo "[STEP 1] Removing old dev.duckdb (if any)"
rm -f dev.duckdb

echo "[STEP 2] Creating fresh dev.duckdb from ddl.sql"
duckdb dev.duckdb < ddl.sql

echo "[STEP 3] Compile SQL without rules"
dbt compile
python3 generate_basic_sqls_wo_optimization.py

echo "[STEP 4] Running dbt"
dbt run

echo "[STEP 3] Compile SQL without rules"
dbt compile
python3 generate_basic_sqls_wo_optimization.py

echo "[STEP 4] Running dbt"
dbt run

echo "[STEP 5] Save dbt run results by querying the table/view"
python3 dbt_run_results_query.py
