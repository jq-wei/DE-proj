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


-- delete all the resourses when done
drop warehouse if exists dbt_wh;
drop database if exists dbt_db;
drop role if exists dbt_role;
```

# create dbt connection to snowflake

use `dbt init`. This will create a project named as e.g. data_pipeline, and a folder in the same name. 

# update dbt_project.yml
set the model as 
```
models:
  data_pipeline:
    # Config indicated by + and applies to all files under models/example/
    staging:
      +materialized: view
      snowflake_warehouse: dbt_wh
    marts:
      +materialized: table
      snowflake_warehouse: dbt_wh
```
which tells snowflake to materialize all the model in staging/ as view, and all the model in marts/ as table.

In this yml, `model-paths: ["models"]` indicates where to look for the model. Model folder contains: SQL logic, source dataset, staging file. 

Good practice: separate stage files, which is one to one source code, from the marts files.


# create packages.yml
install the packages, using `dbt deps`. 

