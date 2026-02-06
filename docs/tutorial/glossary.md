# Data Platform Glossary

A beginner-friendly reference of terms used in this data platform.

## A

### Airflow
Apache Airflow is a workflow orchestration platform. It schedules and monitors data pipelines (DAGs), ensuring tasks run in the correct order and handling retries on failure.

### API (Application Programming Interface)
A way for software applications to communicate with each other. Many data sources expose APIs to retrieve data programmatically.

## B

### Batch Processing
Processing data in groups (batches) at scheduled intervals, rather than one record at a time. Example: Running a daily pipeline that processes all of yesterday's orders.

### Bronze Layer
The first layer in the medallion architecture. Contains raw, unprocessed data exactly as it was received from the source. Stored in MinIO's `raw` bucket.

### Bucket
A container for storing objects (files) in object storage like MinIO or AWS S3. This platform uses three buckets: `raw`, `staging`, and `curated`.

## C

### Checkpoint (Great Expectations)
A configuration that defines which data to validate and which expectation suites to run. Used to execute data quality validation.

### Connection (Airflow)
A stored configuration in Airflow containing credentials and connection details for external systems (databases, cloud storage, APIs).

### CTE (Common Table Expression)
A temporary named result set in SQL that you can reference within a SELECT, INSERT, UPDATE, or DELETE statement. Created with `WITH` clause.

### Curated
Data that has been processed, validated, and is ready for consumption. The gold layer contains curated data.

## D

### DAG (Directed Acyclic Graph)
In Airflow, a DAG is a collection of tasks organized with dependencies. "Directed" means tasks flow in one direction; "Acyclic" means there are no loops.

### Data Catalog
A centralized inventory of all data assets in an organization. OpenMetadata serves as the data catalog in this platform.

### Data Lineage
The tracking of data's origin and all transformations it undergoes. Helps answer "where did this data come from?"

### Data Quality
The measure of data's fitness for its intended use. Includes accuracy, completeness, consistency, timeliness, and validity.

### dbt (data build tool)
A transformation tool that enables data analysts and engineers to transform data using SQL SELECT statements. Models define transformations, and dbt handles execution order.

### Dimension (dim_)
In data modeling, a dimension table contains descriptive attributes used to filter or group facts. Example: `dim_customers` contains customer details.

## E

### ETL (Extract, Transform, Load)
A data integration process: Extract data from sources, Transform it to fit business needs, Load it into a destination system.

### ELT (Extract, Load, Transform)
A variation of ETL where data is loaded first (into a data lake) and transformed afterward. This platform uses ELT.

### Expectation (Great Expectations)
A verifiable assertion about your data. Examples: "this column has no nulls" or "values are between 0 and 100."

### Expectation Suite
A collection of expectations that together define the quality standards for a dataset.

## F

### Fact Table (fct_)
In data modeling, a fact table contains quantitative data (metrics/measures) about business events. Example: `fct_orders` contains order transactions.

## G

### Gold Layer
The final layer in the medallion architecture. Contains aggregated, business-ready data optimized for analytics and reporting. Stored in PostgreSQL `marts` schema.

### Great Expectations
An open-source data quality framework for validating, documenting, and profiling data.

## I

### Idempotent
An operation that produces the same result whether executed once or multiple times. Important for data pipelines to be safely re-runnable.

### Incremental Load
A data loading pattern that only processes new or changed records since the last run, rather than reprocessing everything.

### Ingestion
The process of importing data from external sources into the data platform.

## J

### Job
A unit of work in a data system. In Airflow, a DAG run is a job.

## L

### Lineage
See Data Lineage.

### Logging
Recording events, errors, and metadata about pipeline execution for debugging and auditing purposes.

## M

### Mart (or Data Mart)
A subset of a data warehouse focused on a specific business area. In dbt, `marts` models are final, business-ready tables.

### Materialization (dbt)
How dbt builds a model. Options: `table` (creates a table), `view` (creates a view), `incremental` (appends new rows), `ephemeral` (not materialized).

### Medallion Architecture
A data design pattern organizing data into three layers: Bronze (raw), Silver (cleaned), and Gold (aggregated/analytics-ready).

### Metadata
Data about data. Examples: table schemas, column descriptions, row counts, last updated timestamps.

### MinIO
An S3-compatible object storage server. In this platform, it stores files in Bronze and Silver layers.

### Model (dbt)
A SQL SELECT statement that defines a transformation. dbt converts models into tables or views in the database.

## O

### Object Storage
A storage architecture for managing data as objects (files) rather than blocks or file hierarchies. MinIO and AWS S3 are object storage systems.

### Operator (Airflow)
A template for a task in Airflow. Operators define what a task does: `BashOperator` runs shell commands, `PythonOperator` runs Python functions.

### OpenMetadata
An open-source data catalog and governance platform. Provides data discovery, lineage tracking, and quality monitoring.

### Orchestration
The automated coordination and management of complex workflows. Airflow is an orchestration tool.

## P

### Parquet
A columnar storage file format optimized for analytics. Efficient for queries that only need specific columns.

### Pipeline
A series of data processing steps that move and transform data from source to destination.

### PostgreSQL
An open-source relational database. In this platform, it stores structured data in the Silver and Gold layers.

### Profiling (Data)
Analyzing data to understand its structure, content, and quality. Includes statistics like null counts, unique values, and distributions.

## R

### Raw Data
Unprocessed data in its original form. Stored in the Bronze layer.

### ref() Function (dbt)
A dbt function that references another model, creating a dependency. dbt uses these to determine execution order.

### Runbook
A documented procedure for completing a routine operation or handling a specific situation.

## S

### Schema
The structure of a database or dataset, including table names, column names, and data types.

### Silver Layer
The middle layer in the medallion architecture. Contains cleaned, validated, and standardized data. Stored in PostgreSQL `staging` schema.

### Source (dbt)
Raw data that dbt models transform. Defined in `sources.yml` and referenced with `{{ source() }}`.

### Staging (stg_)
A prefix convention for Silver layer models in dbt. Staging models clean and standardize raw data.

### Superset
Apache Superset is an open-source business intelligence and data visualization platform.

### Surrogate Key
An artificial key generated to uniquely identify a row, typically used instead of natural keys in dimensional modeling.

## T

### Task (Airflow)
A unit of work within a DAG. Tasks are instances of operators that perform specific actions.

### Transformation
The process of converting data from one format or structure to another. dbt handles transformations using SQL.

## V

### Validation
Checking that data meets specified requirements or expectations.

### View
A virtual table based on a SQL query. Views don't store data; they compute results when queried.

## W

### Watermark
A checkpoint indicating the last successfully processed point in data. Used for incremental loading.

### Workflow
A sequence of tasks that accomplish a business process. In this platform, workflows are implemented as Airflow DAGs.

## X

### XCom (Cross-Communication)
An Airflow feature allowing tasks to exchange small amounts of data. Useful for passing results between tasks.

---

## Common Acronyms

| Acronym | Full Form |
|---------|-----------|
| API | Application Programming Interface |
| BI | Business Intelligence |
| CLI | Command Line Interface |
| CTE | Common Table Expression |
| DAG | Directed Acyclic Graph |
| dbt | data build tool |
| DDL | Data Definition Language |
| DML | Data Manipulation Language |
| ELT | Extract, Load, Transform |
| ETL | Extract, Transform, Load |
| GE | Great Expectations |
| OLAP | Online Analytical Processing |
| OLTP | Online Transaction Processing |
| PII | Personally Identifiable Information |
| SQL | Structured Query Language |
| UI | User Interface |
