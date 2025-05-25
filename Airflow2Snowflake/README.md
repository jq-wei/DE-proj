1. Create the following database in snowflake:
   
```
DROP DATABASE IF EXISTS student_database;
CREATE DATABASE student_database;
CREATE WAREHOUSE student_warehouse;
CREATE SCHEMA student_schema;
SELECT * FROM student_info;
```

2. Create airflow connection in the UI, sometime without region works. 