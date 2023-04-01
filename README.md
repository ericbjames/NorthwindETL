# NorthwindETL

1. Start with an introduction: Begin the documentation with an overview of the project, its objectives, and its benefits. Explain what the project aims to achieve and why it is important.

Introduction:
The purpose of this project is to optimize the operations of an e-commerce team by transforming raw data into a more organized and efficient format, and then presenting the findings to the management team. In this project, we will use Databricks to create a pipeline for transforming and storing data in a structured way. The project will consist of three stages: bronze, silver, and gold. 


2. Provide an overview of the data sources: Describe the data sources used in the project, including the format, size, and structure of the data.
The data sources

The Northwind ETL (Extract, Transform, Load) data is a sample database provided by Microsoft that represents a fictional company called Northwind Traders. The database is designed to showcase the features of Microsoft Access, but it has since been adapted for other database management systems, including SQL Server and Databricks


3. Describe the data pipeline: Explain how the data is processed, transformed, and stored in Databricks. Provide details on the tools and technologies used, the data transformations applied, and the storage format and location.

Initial Set up:
The data was ingested from the AWS RDS using airbyte to the databricks datalake initially, and then processed using the medallion architecture.

Bronze Stage:
The bronze stage involves reading in the raw data from the source and transforming it into a structured format. The transformed data is then stored in a Delta table. The code for the bronze stage is provided below:

Silver Stage:

Gold Stage:


4. Discuss the results: Describe the output of the data pipeline, including the transformed data, any performance metrics, and any insights gained from the data analysis.



6. Discuss any limitations or potential issues: Identify any limitations of the project or potential issues that could arise when using the data pipeline.

