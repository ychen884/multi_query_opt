DROP TABLE IF EXISTS "dev"."raw"."raw_customers";
DROP TABLE IF EXISTS "dev"."raw"."raw_items";
DROP TABLE IF EXISTS "dev"."raw"."raw_orders";
DROP TABLE IF EXISTS "dev"."raw"."raw_products";
DROP TABLE IF EXISTS "dev"."raw"."raw_stores";
DROP TABLE IF EXISTS "dev"."raw"."raw_supplies";
DROP SCHEMA IF EXISTS "dev"."raw";

CREATE SCHEMA raw;

CREATE TABLE "dev"."raw"."raw_customers" AS SELECT * FROM read_csv('jaffle-data/raw_customers.csv');
CREATE TABLE "dev"."raw"."raw_items" AS SELECT * FROM read_csv('jaffle-data/raw_items.csv');
CREATE TABLE "dev"."raw"."raw_orders" AS SELECT * FROM read_csv('jaffle-data/raw_orders.csv');
CREATE TABLE "dev"."raw"."raw_products" AS SELECT * FROM read_csv('jaffle-data/raw_products.csv');
CREATE TABLE "dev"."raw"."raw_stores" AS SELECT * FROM read_csv('jaffle-data/raw_stores.csv');
CREATE TABLE "dev"."raw"."raw_supplies" AS SELECT * FROM read_csv('jaffle-data/raw_supplies.csv');
