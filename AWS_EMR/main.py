import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def transform_data(data_source: str, output_url: str) -> None:

    '''
    The steps within this functions:

    1. spark.read.csv() → Loads data into DataFrame df.    
    2. df.select().alias() → Renames columns (creates a new DataFrame).
    3. createOrReplaceTempView() → Registers DataFrame as a SQL table.
    4. spark.sql() → Executes SQL on the current data in the view.
    5. write.parquet() → Saves results.
    '''

    with SparkSession.builder.appName("My first application").getOrCreate() as spark:
        # load CSV file as a dataframe df.
        df = spark.read.option("header", "true").csv(data_source)

        # rename columns
        df = df.select(
            col("Name").alias("name"),
            col("Violation Type").alias("violation_type"),
        )

        # create an in-mem dataframe which makes df a SQL table (containing all the data from csv, 
        # with renamed columns).
        df.createOrReplaceTempView("restaurant_violations")

        # construct SQL query. Note that restaurant_violations is not empty
        GROUP_BY_QUERY = """
            SELECT name, count(*) AS total_red_violations
            FROM restaurant_violations 
            WHERE violation_type = "RED"
            GROUP BY name
        """

        # Transform data. Execute the SQL query on the regitered restaurant_violations view. 
        # Return a new dataframe. Does not modify the original view (read-only).
        transformed_df = spark.sql(GROUP_BY_QUERY) # 

        # log into EMR stdout
        print(f"Number of rows in SQL query: {transformed_df.count()}")

        # writes out results as parquet files
        transformed_df.write.mode("overwrite").parquet(output_url)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_source')
    parser.add_argument('--output_url')
    args = parser.parse_args()

    transform_data(args.data_source, args.output_url)