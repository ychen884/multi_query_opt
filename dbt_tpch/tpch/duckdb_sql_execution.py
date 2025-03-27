#!/usr/bin/env python3
import os
import glob
import duckdb
import time
import csv
import platform
import sys

def drop_views_q01_to_q22(con):
    """
    Drop views named q01..q22 if they exist, ignoring all others.
    """
    for i in range(1, 23):
        view_name = f"q{i:02d}"
        stmt = f"DROP VIEW IF EXISTS {view_name}"
        con.execute(stmt)

def drop_tables_q01_to_q22(con):
    for i in range(1, 23):
        view_name = f"q{i:02d}"
        stmt = f"DROP TABLE IF EXISTS {view_name}"
        con.execute(stmt)

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
def main():
    
    # expect a single argument: either "optimized" or "not_optimized"
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <optimized|not_optimized>")
        sys.exit(1)
    mode = sys.argv[1]
    if mode not in ["optimized", "not_optimized"]:
        print(f"[ERROR] Invalid argument: {mode}")
        print("Please pass either 'optimized' or 'not_optimized'.")
        sys.exit(1)
    
    sql_dir = f"{mode}_sql"
    results_dir = f"{mode}_results"
    db_path = "dev.duckdb"
    sql_dir = "not_optimized_sql"
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

    # Drop any existing q01..q22 tables (or views)
    # drop_tables_q01_to_q22(con)
    # drop_views_q01_to_q22(con)
    sql_files = sorted(glob.glob(os.path.join(sql_dir, "not_optimized_sql_*.sql")))

    # execute each SQL file (these presumably create the q01..q22 tables/views)
    for sql_file in sql_files:
        print(f"[INFO] Executing {sql_file}")
        with open(sql_file, "r") as f:
            sql_statements = f.read()
        try:
            con.execute(sql_statements)
        except Exception as e:
            print(f"[ERROR executing {sql_file}]: {e}")

    # we'll record execution times
    execution_times = []
    os.makedirs(results_dir, exist_ok=True)
    # run SELECT queries on each q01..q22 and collect results + measure performance
    for i in range(1, 23):
        q_name = f"q{i:02d}"
        out_filename = os.path.join(results_dir, f"{q_name}_result.txt")
        select_stmt = f'SELECT * FROM "dev"."main"."{q_name}"'
        try:
            print(f"[INFO] Querying {q_name} -> writing to {out_filename}")
            start_time = time.time()
            results = con.execute(select_stmt).fetchall()
            end_time = time.time()

            elapsed = end_time - start_time
            execution_times.append((q_name, elapsed))

            with open(out_filename, "w") as out_file:
                if results:
                    for row in results:
                        out_file.write(str(row) + "\n")
                else:
                    out_file.write("[No rows returned]\n")

        except Exception as e:
            print(f"[ERROR selecting from {q_name}]: {e}")
            execution_times.append((q_name, None))  # or store some error indicator
            with open(out_filename, "w") as out_file:
                out_file.write(f"[ERROR selecting from {q_name}]: {e}")

    # write execution times
    csv_filename = "benchmark_results.csv"
    with open(csv_filename, "w", newline="") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["QueryName", "ExecutionTimeSeconds"])
        for q_name, t in execution_times:
            # If t is None (error)
            writer.writerow([q_name, t if t is not None else "ERROR"])

    print(f"[INFO] Benchmark results written to {csv_filename}")

    # Close connection
    con.close()

if __name__ == "__main__":
    main()
