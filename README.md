# CitiBike-Forecasting-And-Analysis


## Overview

This repository houses a comprehensive data processing and modeling pipeline designed for bike sharing data. The project employs the medallion format, transforming raw bike and weather information into refined data at bronze, silver, and gold levels of quality. The pipeline encompasses Extract Transform Load (ETL) processes, Exploratory Data Analysis (EDA), Modeling and ML Ops, and Application deployment.

## Extract Transform Load (ETL) Pipeline

### ETL Overview

The ETL pipeline is responsible for processing historical trip and weather data using a Spark streaming job. It not only ingests new data in real-time but also ensures the creation of bronze, silver, and gold level data. The pipeline leverages optimization techniques such as partitioning and Z-ordering for Delta tables.

### Bronze Level

The bronze level captures raw bike and weather data, maintaining the integrity of the original information. This serves as the initial layer of the data refinement process.

### Silver Level

At the silver level, the data is processed to meet the requirements for training features and runtime inference. This layer serves as an intermediate step towards the gold level.

### Gold Level

The gold level encapsulates application and monitoring data. It represents the highest quality of refined data, ready for deployment and real-time monitoring.

### Streaming Definition

The Spark streaming job script defines the schema and data transformations applied to the streaming data. This ensures a standardized and consistent approach to data processing. 

## Exploratory Data Analysis (EDA)

EDA scripts aim to provide insights into the bike sharing data. The analysis covers various aspects, including monthly trip trends, daily trip trends, the impact of holidays on system use, and the influence of weather on daily/hourly trends.

## Modeling and ML Ops

### Forecasting Model

The modeling pipeline builds a forecasting model that infers net bike changes by the hour. Leveraging historical data, the model is designed to provide accurate predictions for system usage.

### MLflow Experiments

Hyperparameter tuning is conducted using MLflow experiments, with Spark trials and hyper-opts applied for optimization. This ensures the model is fine-tuned for optimal performance.

### Model Registry

The model is registered in the Databricks model registry, facilitating seamless transition between staging and production environments.

## Application

### Gold Data Table

The gold data table is a crucial component, storing both inference and monitoring data. It acts as a centralized repository for high-quality, processed data.

### Monitoring

Real-time monitoring components within the application provide insights into system performance, enabling timely interventions if needed.

### Staging vs. Production Monitoring

Comparing actual vs. predicted real-time displays helps ensure the reliability and effectiveness of the models in both staging and production environments.

## Usage

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/shubhamtamhane/CitiBike-Forecasting-And-Analysis.git



### Environment Setup:
Configure your Spark environment and install necessary dependencies as outlined in the documentation.

### Execute the Pipeline:
Run the ETL pipeline, EDA, and modeling scripts in the specified order.

### Deployment:
Deploy the application components as required for monitoring and real-time display.

### Conclusion
This project provides a robust end-to-end solution for processing and modeling bike sharing data. From raw data extraction to real-time monitoring, each component is meticulously designed to ensure data quality, model accuracy, and system reliability. The modular structure allows for easy customization and scalability, making it suitable for a variety of bike sharing scenarios.
