INSTALL tpch;
LOAD tpch;
CALL dbgen(sf = 0.1);

CREATE SCHEMA tpch;

CREATE TABLE "dev"."tpch"."lineitem" AS SELECT * FROM lineitem;
CREATE TABLE "dev"."tpch"."orders" AS SELECT * FROM orders;
CREATE TABLE "dev"."tpch"."customer" AS SELECT * FROM customer;
CREATE TABLE "dev"."tpch"."part" AS SELECT * FROM part;
CREATE TABLE "dev"."tpch"."partsupp" AS SELECT * FROM partsupp;
CREATE TABLE "dev"."tpch"."supplier" AS SELECT * FROM supplier;
CREATE TABLE "dev"."tpch"."nation" AS SELECT * FROM nation;
CREATE TABLE "dev"."tpch"."region" AS SELECT * FROM region;

CREATE SCHEMA raw;

CREATE TABLE "dev"."raw"."raw_customers" AS SELECT * FROM read_csv('jaffle-data/raw_customers.csv');
CREATE TABLE "dev"."raw"."raw_items" AS SELECT * FROM read_csv('jaffle-data/raw_items.csv');
CREATE TABLE "dev"."raw"."raw_orders" AS SELECT * FROM read_csv('jaffle-data/raw_orders.csv');
CREATE TABLE "dev"."raw"."raw_products" AS SELECT * FROM read_csv('jaffle-data/raw_products.csv');
CREATE TABLE "dev"."raw"."raw_stores" AS SELECT * FROM read_csv('jaffle-data/raw_stores.csv');
CREATE TABLE "dev"."raw"."raw_supplies" AS SELECT * FROM read_csv('jaffle-data/raw_supplies.csv');
