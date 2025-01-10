# NYC Taxi Data Pipeline - 2023

This repository contains an end-to-end data engineering project focused on processing and analyzing NYC Taxi data for the year 2023. The project demonstrates key concepts such as data ingestion, transformation, and serving using Azure services and Databricks.

---

## Project Overview

The project is structured using the **Medallion Architecture** (Bronze, Silver, and Gold layers) and incorporates Delta Lake for efficient storage and querying of data.

### Key Components:
1. **Source**:
   - NYC Taxi data for the year 2023.
   - Data is ingested from APIs and stored in raw format.

2. **Ingestion**:
   - Data is ingested into Azure Data Lake Gen2 via Azure Data Factory (ADF).
   - Initial raw data is stored in the **Bronze layer**.

3. **Transformation**:
   - Databricks is used to transform the raw data in stages:
     - **Bronze to Silver**: Cleaning and enriching the data.
     - **Silver to Gold**: Aggregating and preparing the data for analytics.

4. **Serving**:
   - Processed data is stored in the **Gold layer** using Delta Lake for downstream analytics.
   - Data is served for reporting using tools like Power BI.

---

## Pipeline Details

### Azure Data Factory:
- Pipelines configured to ingest NYC Taxi data into the Azure Data Lake Gen2.
- Datasets and linked services are defined for seamless integration with the source and storage.

### Databricks:
- **Notebooks**:
  - `bronze to silver databricks notebook.ipynb`: Handles the transformation of raw data (Bronze layer) to cleaned data (Silver layer).
  - `silver to gold databricks notebook.py`: Converts cleaned data (Silver layer) to aggregated data (Gold layer).
- Transformations include:
  - Data type casting (e.g., long to double, double to long).
  - Calculations such as time differences and other metrics.

### Delta Lake:
- Utilized across the Bronze, Silver, and Gold layers for ACID transactions, schema enforcement, and incremental data ingestion.

---

## Folder Structure

```
|-- dataset/               # Contains sample raw data files.
|-- factory/               # Azure Data Factory pipeline configurations.
|-- linkedService/         # Azure Data Factory linked service configurations.
|-- pipeline/              # Azure Data Factory pipelines.
|-- README.md              # Project documentation.
|-- publish_config.json    # ADF publish configuration.
|-- bronze to silver databricks notebook.ipynb
|-- silver to gold databricks notebook.py
```

---

## Medallion Architecture Overview

### Bronze Layer:
- Raw, unprocessed data ingested from the source.
- Stored in Parquet format in Azure Data Lake Gen2.

### Silver Layer:
- Cleaned and enriched data with consistent data types.
- Intermediate transformations are applied.

### Gold Layer:
- Aggregated and ready-to-use data for analytics and reporting.
- Optimized for performance and downstream applications.

---

## Tools and Technologies

- **Azure Data Lake Gen2**: Cloud storage for raw, transformed, and aggregated data.
- **Azure Data Factory**: Orchestrates data ingestion workflows.
- **Databricks**: Performs data transformations using PySpark.
- **Delta Lake**: Ensures efficient and reliable data storage.
- **Power BI**: For data visualization and reporting.

---

## Instructions to Run

1. **Set up Azure Services**:
   - Create a Data Lake Gen2 storage account.
   - Configure Azure Data Factory with linked services and datasets.

2. **Run ADF Pipelines**:
   - Trigger the pipeline to ingest data from the source into the Bronze layer.

3. **Databricks Notebooks**:
   - Execute the `bronze to silver databricks notebook.ipynb` to clean and transform data.
   - Execute the `silver to gold databricks notebook.py` for final aggregation.

4. **Validate Outputs**:
   - Verify the transformed data in the Gold layer.
   - Use Power BI or other tools for data analysis.

---

## Key Learnings

- Implementing the Medallion Architecture for structured data pipelines.
- Utilizing Delta Lake for robust and scalable data storage.
- Hands-on experience with Azure services and Databricks for ETL processes.

---

## Contributors

- Wilson A (Project Lead)

