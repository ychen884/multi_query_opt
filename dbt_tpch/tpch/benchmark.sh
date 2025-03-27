# 1. init database
# 2. do dbt run
# 3. expected_data = query from result table/view from dbt run
# 4. compile sql without rules vs with our rules & execute them
# 5. compare results for correctness

rm -r ./expected_results
rm -r ./optimized_results
rm -r ./optimized_sql
rm -r ./not_optimized_results
rm -r ./not_optimized_sql
rm ./benchmark_results.csv

dbt clean


# rm -r ./optimized_sql_with_rules

echo "[STEP 1] Removing old dev.duckdb (if any)"
rm -f dev.duckdb

echo "[STEP 2] Creating fresh dev.duckdb from ddl.sql"
duckdb dev.duckdb < ddl.sql

echo "[STEP 3] Running dbt"
dbt run

echo "[STEP 4] Save dbt run results by querying the table/view"
python3 dbt_run_results_query.py

echo "[STEP 5] Compile SQL without rules"
dbt compile
python3 generate_basic_sqls_wo_optimization.py

echo "TODO: [STEP 6] Compile & Execute SQL with rules"
echo "[STEP 7] Execute not optimized SQLs"
python3 duckdb_sql_execution.py not_optimized

echo "[STEP 9] Compare results for correctness (currently only for not optimized version)"
python3 check_correctness.py not_optimized

echo "[INFO] Done."
