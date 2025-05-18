ETL Pipeline using Airflow with openweather API

airflow/ is downloaded from an EC2 instance in AWS

Save the openweather api-key with `airflow variables set OPENWEATHER_API_KEY your_api_key`

Step 1

Create an ec2 fron infra/ using terraform, `terraform apply` which start a tS3.small.

Step 2

In the instance, create a venv using `python -m venv airflow_venv`.

Step 3 

In the venv, install
```
pip install pandas
pip install s3fs
pip install apache-airflow
pip install apache-airflow-providers-http
```

To list the providers, use `airflow providers list`

Then start airflow using `airflow standalone`, the airflow webui will be available at http://pub_ip:8080