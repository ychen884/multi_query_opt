INSTALL tpch;
LOAD tpch;
CALL dbgen(sf = 10);

CREATE SCHEMA tpch;

CREATE TABLE "dev"."tpch"."lineitem" AS SELECT * FROM lineitem;
CREATE TABLE "dev"."tpch"."orders" AS SELECT * FROM orders;
CREATE TABLE "dev"."tpch"."customer" AS SELECT * FROM customer;
CREATE TABLE "dev"."tpch"."part" AS SELECT * FROM part;
CREATE TABLE "dev"."tpch"."partsupp" AS SELECT * FROM partsupp;
CREATE TABLE "dev"."tpch"."supplier" AS SELECT * FROM supplier;
CREATE TABLE "dev"."tpch"."nation" AS SELECT * FROM nation;
CREATE TABLE "dev"."tpch"."region" AS SELECT * FROM region;