
### Multi-Query Optimization (MQO) with dbt (initial logical optimization stage)

This repository aims for initial logical optimization stage for a MQO project:
- dbt (Data Build Tool) for high-level SQL model definitions & DAG management
- SQLGlot for parsing and rewriting queries
- (Optionally?) DataFusion or another query engine for execution

### Slide for proposal presentation
https://docs.google.com/presentation/d/1XBczAdjLrZeNcwHgPrc8DBltmGDPLwNbDe8ynEbQtuM/edit?usp=sharing


#### Project Structure

- **dbt_project.yml**: Main dbt config for this project.
- **models/**: All dbt models are organized here.
  - **example/**: Original sample with two basic models and a schema.yml.
  - **MQO_1/**: Three models (`my_raw_orders.sql`, `recent_orders.sql`, `daily_aggregates.sql`) demonstrating repeated filters.
- **parse_dbt_manifest*.py**: Python scripts that load dbt’s manifest.json, build a DAG (networkx), parse the compiled SQL (SQLGlot), and optionally filter by directory.
(Let's say that each model directory is a seperate example, we do not consider them as a single query batche)
- **dev.duckdb**: Local DuckDB file for storing tables.

#### High-Level Workflow for P2 initial logical optimization stage

1. **Author Models in dbt**  
   - Place SQL in `models/MQO_1/` or `models/example/`.
   - Use `{{ ref('other_model') }}` for dependencies.

2. **Compile / Run dbt**  
   - `dbt compile` or `dbt run` produces:
     - `target/manifest.json` (the DAG)
     - compiled SQL in `target/compiled/`

3. **Parse Manifest & Analyze**  
   - `parse_dbt_manifest.py` shows how to:
     - Load manifest.json
     - Build a dependency graph (networkx)
     - Parse each compiled SQL with SQLGlot
   - Potentially detect shared filters or subexpressions for multi-query optimization.
   - TODO: Now we have a basic parsing phase, we may need to figure out how to do rewrite later

4. **(Optional) Execute on Query Engine**  
   - If we implement a rewrite, we can feed it to DuckDB, DataFusion, or another engine to measure performance gains.

#### Setup & Installation

1. **Clone & Enter**  
   git clone
   cd multi_query_opt

2. **Install Python Dependencies**  
   pip install dbt-core dbt-duckdb networkx sqlglot

3. **Configure dbt (I didn't try, I use the default, below is GPT generated)**
   - Create or edit `~/.dbt/profiles.yml` for a `multi_query_opt` profile. For DuckDB:
     multi_query_opt:
       target: dev
       outputs:
         dev:
           type: duckdb
           path: "dev.duckdb"
           threads: 4

4. **Compile / Run**  
   dbt compile  
   or  
   dbt run  
   or specify our example like  
   dbt compile --select models/MQO_1/* --no-partial-parse

5. **Use Python Scripts**  
   python3 parse_dbt_manifest_use_parse_all_model_dirs.py  
   or   
   python3 parse_dbt_manifest_select_model_dir.py   
   - This parses the manifest, prints a topological order of models, and shows SQLGlot ASTs for each compiled query.
   - The parse_dbt_manifest_select_model_dir will parse graph only in specified dir(hardcoded dir now in code), cuz it's just a demo now

#### Notes

- **Partial Parsing**: dbt may retain older models in the manifest. Use `dbt clean` or `--no-partial-parse` to force a fresh parse.
- **Selective Compilation**: dbt compile --select models/MQO_1/* restricts to that directory.
- **Multi-Query Optimization**: Look for repeated subqueries or filters among multiple dbt models. Then unify them into a single materialized view to save computation.


#### Common MQO Rewrite Strategies

Below are potential logical transformations or rewrite rules we might apply after parsing dbt’s compiled queries with SQLGlot (or any other AST framework):

1. **Shared Predicate / Sub-Scan**  
   - **Scenario**: Multiple queries filter the same table on the same condition:
     ```
     SELECT ... FROM orders WHERE order_date >= '2023-01-01';
     SELECT ... FROM orders WHERE order_date >= '2023-01-01';
     ```
   - **Rewrite**: Create a **temp view** or a single sub-scan of `orders` with that filter, then reference it in both queries:
     ```
     CREATE TEMP VIEW recent_orders AS
     SELECT * FROM orders WHERE order_date >= '2023-01-01';

     -- Then each query references `recent_orders`.
     ```
   - This avoids scanning `orders` multiple times.

2. **Projection Pushdown**  
   - **Scenario**: Queries request only a subset of columns, but the underlying sub-plan is selecting all columns.
   - **Rewrite**: Eliminate unnecessary columns early in the query plan, ensuring minimal data movement:
     ```
     -- Instead of
     SELECT * FROM (SELECT * FROM orders WHERE ...[<- say that this is reference to another query>] )
     -- Do
     SELECT customer_id, order_date
     FROM (SELECT customer_id, order_date FROM orders WHERE ...)
     ```
   - Speeds up by reading fewer columns from disk and memory.

3. **Common Subexpression Elimination (CSE)**  
   - **Scenario**: Multiple queries (or sub-parts of a single query) compute the **same** expression or sub-result, e.g. complicated grouping or repeated window functions.
   - This may look like 1, but just saying it is for a more general repeated sub-plan or expression—could be any repeated logic (filters, joins, aggregations, window functions, etc.).
   If we make 1 work, then this is a more generic case.
   - **Rewrite**: Evaluate that expression once and store it in a **CTE** (Common Table Expression) or a **temp view**:
     ```
     WITH repeated_calc AS (
       SELECT customer_id, some_complex_agg(...) as metric
       FROM ...
     )
     SELECT ...
     FROM repeated_calc
     ```
   - Then reuse `repeated_calc` across multiple references.

4. **Join Sharing or Reordering**  
   - **Scenario**: Multiple queries join the same two (or more) tables. 
   - **Rewrite**: If the join is expensive, we can do it **once** and reference the joined result if the subsequent queries require the **exact same** join condition. Or reorder the joins for better efficiency(I suspect reorder is not necessary though):
     ```
     -- Instead of two separate queries doing the same join
     -- unify them into a single pipeline or share a joined temp view.
     ```

5. **Predicate Splitting or Combining**  
   - **Scenario**: Two queries have overlapping filters, e.g.
     ```
     WHERE x > 10 AND y = 'A'
     WHERE x > 5  AND y = 'A'
     ```
   - Similar to 1, but it's overlapping 
   - If it’s beneficial, we might unify partial conditions or rewrite them to handle the superset of rows, then apply an additional filter later. This depends on the cost model.

6. **Materialized Aggregates**  
   - **Scenario**: Repeated aggregations on the same dimension (e.g., daily or monthly sums).  
   - **Rewrite**: Build a **materialized table** of aggregated results once, then queries read from that. 
     ```
     CREATE TABLE daily_agg AS
     SELECT date, SUM(amount) as total
     FROM orders
     GROUP BY date;

     -- All queries referencing daily aggregates can read from `daily_agg`.
     ```

7. **Null Suppression / Filter Removal**  
   - If queries do unnecessary filters or test for conditions that are guaranteed by schema constraints, we can remove them. Similarly, if `NOT NULL` is enforced in the schema, we can drop extraneous `WHERE x IS NOT NULL`.
   - A uses B, and B has already filtered NOT NULL, then no need for A to filter it out

8. **Window Function Unification**  
   - **Scenario**: Multiple queries use the same window function partitioning and ordering.  
   - **Rewrite**: Evaluate it once, store the results in a temp table, then read from that for subsequent queries.
