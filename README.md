# PySpark-Project: IPL Data Analysis with PySpark and Databricks

This project demonstrates how to analyze IPL (Indian Premier League) data using PySpark within a Databricks environment. The dataset is hosted on AWS S3, and the analysis is performed using PySpark functionality within a Databricks notebook.

## Project Overview

1. **Dataset**: 
   - **Source**: [IPL Data till 2017](https://data.world/raghu543/ipl-data-till-2017/workspace/project-summary?agentid=raghu543&datasetid=ipl-data-till-2017)
   - **Description**: The dataset contains information related to IPL matches, teams, and players till the year 2017.

2. **Data Storage**:
   - **AWS S3 Bucket**: The dataset is stored in my Amazon S3 bucket.

3. **Environment Setup**:
   - **Databricks**: Created a Databricks account (Community Edition).
   - **Compute**: Set up a compute resource within Databricks.
   - **Notebook**: Created and executed a notebook containing the analysis code.

4. **Code Execution**:
   - The analysis code is provided in the repository. It demonstrates how to use PySpark for data manipulation and visualization.

## PySpark Functions Used

The following PySpark functions and methods were utilized in the notebook:

1. **Spark Session**:
   ```python
   spark = SparkSession.builder.appName("IPL Data Analysis").getOrCreate()
   ```

2. **Reading Data**:
   ```python
   df = spark.read.options("header", "true").csv(PATH, inferSchema=True)
   df = spark.read.csv(PATH, header=True, inferSchema=True)
   ```

3. **Displaying Data**:
   ```python
   df.show(numRows, truncate=False)
   ```

4. **Data Transformation**:
   ```python
   df = df.withColumn("new_col", df["old_col"] * some_expression)
   ```

5. **SQL Queries**:
   - Registering DataFrame as a SQL table:
     ```python
     df.createOrReplaceTempView("table_name")
     ```
   - Executing SQL queries:
     ```python
     result = spark.sql("SELECT * FROM table_name WHERE condition")
     ```

6. **Data Aggregation**:
   ```python
   df.groupBy("column").agg(sum("column"), avg("column"))
   ```

7. **Data Manipulation**:
   - Filling missing values:
     ```python
     df = df.fillna(value)
     ```
   - Conditional Expressions:
     ```python
     df = df.withColumn("new_col", when(df["condition"].isNull(), "default_value").otherwise(df["existing_col"]))
     ```

8. **Date and Time Functions**:
   ```python
   df = df.withColumn("year", year(df["date_column"]))
   df = df.withColumn("month", month(df["date_column"]))
   df = df.withColumn("day", dayofmonth(df["date_column"]))
   ```

9. **Window Functions**:
   ```python
   from pyspark.sql.window import Window
   windowSpec = Window.partitionBy("column").orderBy("column")
   df = df.withColumn("rank", rank().over(windowSpec))
   ```

10. **Schema Definition**:
    ```python
    from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType, DecimalType, BooleanType
    schema = StructType([
        StructField("column_name", IntegerType(), True),
        ...
    ])
    ```

11. **Visualization**:
    - Plotted a graph using `matplotlib` to visualize SQL query results.

## Acknowledgements

- IPL dataset provided by [Raghu](https://data.world/raghu543/ipl-data-till-2017/workspace/project-summary?agentid=raghu543&datasetid=ipl-data-till-2017).
- AWS S3 for data storage.
- Databricks Community Edition for running the analysis.
