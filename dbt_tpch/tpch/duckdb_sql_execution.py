#!/usr/bin/env python3
import os
import glob
import duckdb

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

def main():
    db_path = "dev.duckdb"

    sql_dir = "optimized_sql"

    con = duckdb.connect(db_path)

    # drop any existing q01..q22 views
    # drop_views_q01_to_q22(con)
    drop_tables_q01_to_q22(con)

    sql_files = sorted(glob.glob(os.path.join(sql_dir, "optimized_sql_*.sql")))
    for sql_file in sql_files:
        print(f"[INFO] Executing {sql_file}")
        with open(sql_file, "r") as f:
            sql_statements = f.read()
        try:
            con.execute(sql_statements)
        except Exception as e:
            print(f"[ERROR executing {sql_file}]: {e}")

    # for each q01..q22, select data and write to separate txt file (qxx_result.txt)
    for i in range(1, 23):
        q_name = f"q{i:02d}"
        # if optimized_results folder does not exist, create it
        os.makedirs("optimized_results", exist_ok=True)
        
        out_filename = f"./optimized_results/{q_name}_result.txt"

        select_stmt = f'SELECT * FROM "dev"."main"."{q_name}"'

        try:
            print(f"[INFO] Querying {q_name} -> writing to {out_filename}")
            results = con.execute(select_stmt).fetchall()

            with open(out_filename, "w") as out_file:
                if results:
                    for row in results:
                        out_file.write(str(row) + "\n")
                else:
                    out_file.write("[No rows returned]\n")
        except Exception as e:
            print(f"[ERROR selecting from {q_name}]: {e}")
            with open(out_filename, "w") as out_file:
                out_file.write(f"[ERROR selecting from {q_name}]: {e}")

    # Close connection
    con.close()

if __name__ == "__main__":
    main()
