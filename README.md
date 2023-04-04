# NorthwindETL

1. Start with an introduction: Begin the documentation with an overview of the project, its objectives, and its benefits. Explain what the project aims to achieve and why it is important.

## Project Architecture
![Screenshot 2023-04-03 at 11 41 36 PM](https://user-images.githubusercontent.com/101911329/229883209-f24c71ab-f562-41ef-b07f-0a62ceebada3.png)


Introduction:
The purpose of this project is to optimize the operations of an e-commerce team by transforming raw data into a more organized and efficient format, and then presenting the findings to the management team. In this project, we will use Databricks to create a pipeline for transforming and storing data in a structured way. The project will consist of three stages: bronze, silver, and gold. 


2. Provide an overview of the data sources: Describe the data sources used in the project, including the format, size, and structure of the data.
The data sources

The Northwind ETL (Extract, Transform, Load) data is a sample database provided by Microsoft that represents a fictional company called Northwind Traders. The database is designed to showcase the features of Microsoft Access, but it has since been adapted for other database management systems, including SQL Server and Databricks


3. Describe the data pipeline: Explain how the data is processed, transformed, and stored in Databricks. Provide details on the tools and technologies used, the data transformations applied, and the storage format and location.

Initial Set up:
The data was ingested from the AWS RDS using airbyte to the databricks datalake initially, and then processed using the medallion architecture.

Bronze Stage:
The bronze stage involves reading in the raw data from the source and transforming it into a structured format. The transformed data is then stored in a Delta table. 

 A large number of tables were provided from the Northwind ETL database, however only the tables below are chosen to be processed further:
- products
- orders
- orders_details
- categories

Silver Stage:

## Dimensional Model:

![Screenshot 2023-04-04 at 7 06 19 AM](https://user-images.githubusercontent.com/101911329/229883120-feedd709-a170-4352-ae5c-b3c88672e2e4.png)

## Databricks Workflow:
![Screenshot 2023-04-03 at 11 13 37 PM](https://user-images.githubusercontent.com/101911329/229883294-2b871fbd-3ec4-40a8-80e3-39b139f9890d.png)

4. Discuss the results: Describe the output of the data pipeline, including the transformed data, any performance metrics, and any insights gained from the data analysis.

## Final Dashboard
![Screenshot 2023-04-03 at 11 07 38 PM](https://user-images.githubusercontent.com/101911329/229883426-923b9d5a-99da-48cf-a7ed-0a2d64cd07c1.png)


6. Discuss any limitations or potential issues: Identify any limitations of the project or potential issues that could arise when using the data pipeline.

