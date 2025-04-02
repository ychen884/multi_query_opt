bash ./cleanup.sh
dbt clean
# rm -r ./optimized_sql_with_rules

echo "[STEP 1] Removing old dev.duckdb (if any)"
rm -f dev.duckdb

echo "[STEP 2] Creating fresh dev.duckdb from ddl.sql"
duckdb dev.duckdb < ddl.sql

echo "[STEP 3] Compile SQL without rules"
dbt compile
python3 generate_basic_sqls_wo_optimization.py

echo "[STEP 4] Optimize SQL with our rules"
python3 parse_dbt_manifest_select_model_dir.py

echo "[STEP 8] Execute not optimized SQLs"
python3 duckdb_sql_execution.py not_optimized --no-save-results

echo "[STEP 7] Execute optimized SQLs & save performance results"
python3 duckdb_sql_execution.py optimized --no-save-results



echo "[STEP 11] Compare performance for optimized SQLs"
python3 compare_final_perf.py ./unoptimized_bench_mark_results.csv ./optimized_bench_mark_results.csv ./final_benchmark_results.csv

echo "[INFO] Done."
