# Component Name

## Overview

>What motivates this to be implemented? What will this component achieve? 

### Context
dbt (Data Build Tool) is a widely used SQL-centric tool for data transformation. Given a set of SQL files as input, dbt compiles them into a Directed Acyclic Graph (DAG) by analyzing user-defined dependencies (for example, SQL file A might depend on the table produced by SQL file B). Then dbt executes each node in this DAG directly, producing materialized tables or views.

However, dbt itself is not designed to optimize these SQL queries before execution. Therefore, there are opportunities for multi-query optimizations that could further improve performance.

### Goal
Our main goal is to explore ways of rewriting the DAG (i.e., the compiled SQL queries) to achieve better query execution performance. We have three milestone goals:

In this project, we have three milestone goals:

#### 75% goal: 
- Generate a workload for benchmarking dbt’s DAGs
- Implement a DAG rewriter with at least the predicate pushdown heuristic

#### 100% goal:
- Implement all the proposed logical query optimizations for multiple queries
- Evaluate the DAG using the benchmark

#### 125% goal:
- Explore and implement Physical query optimization (e.g. cache & reuse of intermediate “subresults”)

## Scope
>Which parts of the system will this feature rely on or modify? Write down specifics so people involved can review the design doc

## Glossary (Optional)

>If you are introducing new concepts or giving unintuitive names to components, write them down here.

## Architectural Design
>Explain the input and output of the component, describe interactions and breakdown the smaller components if any. Include diagrams if appropriate.

#### Execution module
- We decided to implement our own execution module, separate from dbt. This choice allows us to focus on DAG rewriting while having stronger control over execution and workload evaluation.

1. Obtain a topological ordering of the DAG’s nodes (each node corresponds to a single SQL query). This order could be changed afte optimization because the DAG could get changed.
2. Execute the queries in DuckDB in that topological order. Each query is typically doing a materialization for a table or view.
3. The details of this module’s design and interaction with DuckDB are described in the Testing Plan section below.

## Design Rationale
>Explain the goals of this design and how the design achieves these goals. Present alternatives considered and document why they are not chosen.

## Testing Plan
We will measure both the correctness and the performance of the rewritten DAG compared to the original DAG.

Because there are few open-source DAG benchmark workloads, we plan to create our own, inspired by TPC-H. We will load TPC-H tables as the underlying raw data, then build data-model queries from the standard TPC-H queries. For each of our rewrite rules, we will construct DAGs that can benefit from that rule, thereby demonstrating the resulting performance improvement.

### Correctness check
To verify that our optimized SQL produces the same results:
1. We do `dbt run` normally and query the materialzed results to record the materialized tables/views. This is the expected results. 
2. We apply our optimizations to rewrite the DAG.  
3. We execute the optimized DAG and compare the final materialized tables/views with the original outputs (by running a separate query to fetch all rows).


### Performance evaluation and benchmark
1. We rely on DuckDB’s internal profiling command `EXPLAIN ANALYZE` for multiple runs. Each run uses a cold cache (i.e., a fresh session) to ensure consistent timing. After these runs, we collected and compared the execution time (sum, avg, tail, etcs). 
2. We found that DuckDB may automatically scale different numbers of threads based on running environment. To prevent inconsistent results due to concurrency, we set a fixed number of threads (currently 1) for every run.
3. For each rewrite rule we develop, we would create a specialized set of DAGs that could benefit from that rule.


## Trade-offs and Potential Problems
>Write down any conscious trade-off you made that can be problematic in the future, or any problems discovered during the design process that remain unaddressed (technical debts).


## Future Work
>Write down future work to fix known problems or otherwise improve the component.
