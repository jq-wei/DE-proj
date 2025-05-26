Local dev using dbt core to snowflake with airflow. 

1. create a new venv with `python -m venv dbt_snow`
2. Step up the venv: 

```
pip install dbt-snowflake
pip install dbt-core
```

# setup snowflake
create database, warehouse, role etc

```SQL
use role accountadmin;

create warehouse dbt_wh with warehouse_size = "x-small";
create database if not exists dbt_db;
create role if not exists dbt_role;

show grants on warehouse dbt_wh;

grant usage on warehouse dbt_wh to role dbt_role; --grant usage previlige
grant role dbt_role to user my_cool_username;
grant all on database dbt_db to role dbt_role;
 
use role dbt_role; -- switch to the newly created role

create schema dbt_db.dbt_schema;

```

