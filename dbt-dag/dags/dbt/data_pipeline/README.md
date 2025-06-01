Welcome to your new dbt project!


### setup snowflake env. run with snowflake worksheet

```sql
use role ACCOUNTADMIN;

create warehouse dbt_wh with warehouse_size = "x-small";
create database if not exists dbt_db;
create role if not exists dbt_role;

show grants on warehouse dbt_wh;

grant usage on warehouse dbt_wh to role dbt_role;
grant role dbt_role to user admin;
grant all on database dbt_db to role dbt_role;

use role dbt_role;
create schema dbt_db.dbt_schema;
```


### Using the starter project

Try running the following commands:
- dbt run
- dbt test
- run a single model, `dbt run -s stg_tpch_line_items`


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices


### transform tables

transform tables, the staging tables which are one-to-one corresponding to source table. 

aggregate line_items table to create some fact tables. 

This one goes into /marts  models.

First run `dbt run -s stg_tpch_orders` then `dbt run -s int_order_items`


### Marco

Marco funcs are to reuse bussiness logic.

### Deployment: airflow

Use [Cosmos](https://github.com/astronomer/astronomer-cosmos) which can run dbt Core projects as Apache Airflow DAGs.

Install in Linux using `curl -sSL install.astronomer.io | sudo bash -s`.

Init project using 
```bash
mkdir dbt-dag
cd dbt-dag/
astro dev init
```

Then update Dockerfile and requirements.txt and run 
`astro dev start`
with output `Airflow is starting up!`