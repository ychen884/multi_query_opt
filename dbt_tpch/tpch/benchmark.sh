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
rm materialized_tables.txt
rm materialized_tables_optimized.txt
rm topo_sort_order.txt
rm topo_sort_order_optimized.txt

dbt clean
# rm -r ./optimized_sql_with_rules

echo "[STEP 1] Removing old dev.duckdb (if any)"
rm -f dev.duckdb

echo "[STEP 2] Creating fresh dev.duckdb from ddl.sql"
duckdb dev.duckdb < ddl.sql

echo "[STEP 3] Compile SQL without rules"
dbt compile
python3 generate_basic_sqls_wo_optimization.py

echo "[STEP 4] Running dbt"
dbt run

echo "[STEP 5] Save dbt run results by querying the table/view"
python3 dbt_run_results_query.py

echo "[STEP 6] Optimize SQL with our rules"
python3 parse_dbt_manifest_select_model_dir.py

echo "[STEP 7] Execute optimized SQLs & save performance results"
python3 duckdb_sql_execution.py optimized

echo "[STEP 7] Execute not optimized SQLs"
python3 duckdb_sql_execution.py not_optimized

echo "[STEP 8] Execute optimized SQLs"
python3 python3 parse_dbt_manifest_select_model_dir.py

echo "[STEP 9] Compare results for correctness for not optimized SQLs"
python3 check_correctness.py not_optimized

echo "[STEP 10] Compare results for correctness for optimized SQLs"
python3 check_correctness.py optimized

echp "[STEP 11] Compare performance for optimized SQLs"
python3 compare_final_perf.py ./unoptimized_bench_mark_results.csv ./optimized_bench_mark_results.csv ./final_benchmark_results.csv

echo "[INFO] Done."
