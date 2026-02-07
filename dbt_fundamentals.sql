SELECT
  *
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
WHERE lower(table_schema) in ('dbt_kdeyette','jaffle_shop','stripe')
ORDER BY table_catalog, table_schema, table_name;

SELECT DISTINCT
  catalog_name AS database_name,
  schema_name
  --, s.*
FROM SNOWFLAKE.ACCOUNT_USAGE.SCHEMATA s
WHERE LOWER(schema_name) IN ('jaffle_shop','stripe','dbt_kdeyette')
ORDER BY database_name, s.schema_name;

SELECT
  c.table_catalog,
  c.table_schema,
  c.table_name,
  c.column_name,
  c.ordinal_position,
  c.data_type,
  c.is_nullable,
  c.comment
FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS c
WHERE LOWER(c.table_schema) IN ('dbt_kdeyette','jaffle_shop','stripe')
ORDER BY c.table_catalog, c.table_schema, c.table_name, c.ordinal_position;

create warehouse transforming; 
create database raw; 
create database analytics; 
create schema raw.jaffle_shop; 
create schema raw.stripe;

create table raw.jaffle_shop.customers 
( id integer,
  first_name varchar,
  last_name varchar
);

copy into raw.jaffle_shop.customers (id, first_name, last_name)
from 's3://dbt-tutorial-public/jaffle_shop_customers.csv'
file_format = (
    type = 'CSV'
    field_delimiter = ','
    skip_header = 1
    );

create table raw.jaffle_shop.orders
( id integer,
  user_id integer,
  order_date date,
  status varchar,
  _etl_loaded_at timestamp default current_timestamp
);

copy into raw.jaffle_shop.orders (id, user_id, order_date, status)
from 's3://dbt-tutorial-public/jaffle_shop_orders.csv'
file_format = (
    type = 'CSV'
    field_delimiter = ','
    skip_header = 1
    );


create table raw.stripe.payment 
( id integer,
  orderid integer,
  paymentmethod varchar,
  status varchar,
  amount integer,
  created date,
  _batched_at timestamp default current_timestamp
);

copy into raw.stripe.payment (id, orderid, paymentmethod, status, amount, created)
from 's3://dbt-tutorial-public/stripe_payments.csv'
file_format = (
    type = 'CSV'
    field_delimiter = ','
    skip_header = 1
    );

select * from raw.jaffle_shop.customers ;

select * from raw.jaffle_shop.orders; 

select * from raw.stripe.payment;