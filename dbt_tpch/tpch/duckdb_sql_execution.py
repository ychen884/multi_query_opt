#!/usr/bin/env python3
import os
import glob
import duckdb
import time
import csv
import platform
import sys


# init database
def execute_ddl_data_init(con, ddl_file_path):
    with open(ddl_file_path, "r") as f:
        ddl_script = f.read()
    print("Init data from ddl file:" + ddl_file_path)
    con.execute(ddl_script)


# run env
def summarize_cpuinfo_linux():
    if not os.path.exists("/proc/cpuinfo"):
        return "[WARN] /proc/cpuinfo not found on this system."

    model_name = None
    total_logical = 0
    physical_ids = set()
    cores_by_physid = {}

    current_physid = None

    with open("/proc/cpuinfo", "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                current_physid = None
                continue

            if line.startswith("processor"):
                total_logical += 1
            elif line.startswith("model name") and model_name is None:
                model_name = line.split(":", 1)[1].strip()
            elif line.startswith("physical id"):
                current_physid = line.split(":", 1)[1].strip()
                physical_ids.add(current_physid)
            elif line.startswith("cpu cores") and current_physid is not None:
                cpu_cores = line.split(":", 1)[1].strip()
                cores_by_physid[current_physid] = cpu_cores

    num_packages = len(physical_ids)
    if model_name is None:
        model_name = "Unknown CPU"
    unique_cores = set(cores_by_physid.values())
    if len(unique_cores) == 1:
        # e.g. "10"
        core_count_str = next(iter(unique_cores))
        summary = (f"{num_packages} × ({model_name}, {core_count_str} cores, "
                   f"{total_logical} threads total)")
    else:
        summary = f"{num_packages} packages of {model_name}, total logical: {total_logical}.\n"
        for pid in sorted(cores_by_physid):
            summary += f"  - Physical ID {pid}, {cores_by_physid[pid]} cores\n"

    return "[INFO] CPU Summary: " + summary


def run_explain_analyze_and_parse_time(db_path, sql):
    """
    Create a fresh connection, run EXPLAIN ANALYZE on the given SQL, parse the
    reported total time from DuckDB output, and return it in nanoseconds.
    Return -1 if we fail to parse the time.
    """
    # open new connection to avoid caching from previous run
    con = duckdb.connect(db_path)
    con.execute("SET threads = 1;")
    # We wrap the original SQL with EXPLAIN ANALYZE
    explain_query = f"EXPLAIN ANALYZE {sql}"
    try:
        rows = con.execute(explain_query).fetchall()
        total_time_ms = -1
        for row in rows:
            for plan_line in row:
                # print(plan_line)
                plan_line_lower = plan_line.lower()
                if "total time:" in plan_line_lower:
                    # print("Parsed perf: ", plan_line_lower)
                    right = plan_line_lower.split("total time:", 1)[1].strip()
                    # right now is "0.0014s             ││..."
                    # need to extract the number and unit
                    # e.g. "0.0014s"
                    # split by whitespace
                    right = right.split()[0]
                    print(f"[INFO] Parsed perf: {right}")
                    val = ""
                    unit = ""
                    for c in right:
                        if c.isdigit() or c == ".":
                            val += c
                        else:
                            unit += c
                    val = val.strip()
                    unit = unit.strip()
                    try:
                        float_val = float(val)
                        if unit.startswith("s"):
                            total_time_ms = int(float_val * 1e3)
                        elif unit.startswith("ms"):
                            total_time_ms = int(float_val)
                        else:
                            print(f"[WARN] Unknown unit: {unit}")
                            total_time_ms = -1
                    except:
                        pass

                    return total_time_ms
    except Exception as e:
        print(f"[ERROR] EXPLAIN ANALYZE failed: {e}")
        return -1
    finally:
        con.close()


def main():

    # set the number of executions
    exec_ct = 2
    
    # expect a single argument: either "optimized" or "not_optimized"
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <optimized|not_optimized>")
        sys.exit(1)
    mode = sys.argv[1]
    if mode not in ["optimized", "not_optimized"]:
        print(f"[ERROR] Invalid argument: {mode}")
        print("Please pass either 'optimized' or 'not_optimized'.")
        sys.exit(1)
    skip_results = "--no-save-results" in sys.argv
    sql_dir = f"{mode}_sql"
    results_dir = f"{mode}_results"
    db_path = "dev.duckdb"
    ddl_script = "ddl.sql"
    machine_info = platform.uname()
   
    print("[INFO] Machine info:")
    print(f"  System:    {machine_info.system} {machine_info.release}")
    print(f"  Machine:   {machine_info.machine}")
    print(f"  Processor: {machine_info.processor}")
    # if in linux:
    if machine_info.system == "Linux":
        print(summarize_cpuinfo_linux())
    else:
        print("[INFO] Should manually check CPU info")
    print("")
    if os.path.exists("dev.duckdb"):
        print("Clearing previous database file for new performance evaluation")
        os.remove("dev.duckdb")


    con = duckdb.connect(db_path)
    execute_ddl_data_init(con, ddl_script)
    con.close()

    topo_sort_file = "topo_sort_order.txt"
    if(mode == "optimized"):
        topo_sort_file = "topo_sort_order_optimized.txt"
    materialized_tables_file = "materialized_tables.txt"
    if(mode == "optimized"):
        materialized_tables_file = "materialized_tables_optimized.txt"

    with open(topo_sort_file, "r") as f:
        sql_files = [line.strip() for line in f if line.strip()]
    with open(materialized_tables_file, "r") as f:
        sql_out_tables = [line.strip() for line in f if line.strip()]

    creation_times = []

    for idx, sql_file in enumerate(sql_files):
        out_table = sql_out_tables[idx]
        print(f"[INFO] Preparing to run {sql_file} (output table: {out_table})")
        with open(sql_file, "r") as f:
            sql_statements = f.read()

        total_time_ms = 0
        for _ in range(exec_ct):
            # new connection to avoid reuse caching 
            tmp_con = duckdb.connect(db_path)
            
            drop_stmt = f"DROP TABLE IF EXISTS {out_table}"
            try:
                tmp_con.execute(drop_stmt)
            except Exception as e:
                print(f"[WARN] Error dropping table {out_table}: {e}")
            finally:
                tmp_con.close()

            # measure execution time with EXPLAIN ANALYZE
            # note that the query presumably creates table {out_table}
            query_time_ns = run_explain_analyze_and_parse_time(db_path, sql_statements)

            if query_time_ns < 0:
                print(f"[ERROR] Failed to parse duckdb query profiling time for {sql_file}.")
                fallback_con = duckdb.connect(db_path)
                drop_stmt = f"DROP TABLE IF EXISTS {out_table}"
                try:
                    fallback_con.execute(drop_stmt)
                except:
                    pass
                start_ns = time.time_ns()
                try:
                    fallback_con.execute(sql_statements)
                except Exception as ex:
                    print(f"[ERROR executing {sql_file} in fallback mode]: {ex}")
                    # if still failed, simply set time as -1
                    total_time_ms = -1
                    break
                end_ns = time.time_ns()
                fallback_con.close()

                query_time_ns = end_ns - start_ns

            total_time_ms += query_time_ns

        # make out table name more readable
        tb_name = out_table.replace('"', '').split('.')[-1]
        # record the total creation time across 10 runs
        creation_times.append((tb_name, total_time_ms))

    if mode == "optimized":
        creation_csv = "optimized_bench_mark_results.csv"
    else:
        creation_csv = "unoptimized_bench_mark_results.csv"

    with open(creation_csv, "w", newline="") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["TableName", "TotalCreationTimeMs(10runs)"])
        for table_name, total_ns in creation_times:
            writer.writerow([table_name, total_ns])
    if not skip_results:
        os.makedirs(results_dir, exist_ok=True)

        final_con = duckdb.connect(db_path)
        with open(materialized_tables_file, "r") as f:
            lines = f.readlines()

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # line example:  "dev"."main"."q06"
            table_fqn = line
            cleaned = table_fqn.replace('"', '')
            final_name = cleaned.split('.')[-1]
            out_filename = os.path.join(results_dir, f"{final_name}.result")
            select_stmt = f"SELECT * FROM {table_fqn}"

            try:
                print(f"[INFO] Querying {table_fqn} -> writing to {out_filename}")
                results = final_con.execute(select_stmt).fetchall()
                with open(out_filename, "w") as out_file:
                    if results:
                        for row in results:
                            out_file.write(str(row) + "\n")
                    else:
                        out_file.write("[No rows returned]\n")

            except Exception as e:
                print(f"[ERROR selecting from {table_fqn}]: {e}")
                with open(out_filename, "w") as out_file:
                    out_file.write(f"[ERROR selecting from {table_fqn}]: {e}")

        final_con.close()


if __name__ == "__main__":
    main()
