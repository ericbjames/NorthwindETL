# NorthwindETL

Introduction:
The purpose of this project is to optimize the operations of an e-commerce team by transforming raw data into a more organized and efficient format, and then presenting the findings to the management team. The pipeline follows a bronze-silver-gold architecture and uses Databricks for data processing. The final output of the pipeline is a set of tables and visualizations that provide valuable insights into the e-commerce operations of Northwind Traders.


## Project Architecture
![Screenshot 2023-04-03 at 11 41 36 PM](https://user-images.githubusercontent.com/101911329/229883209-f24c71ab-f562-41ef-b07f-0a62ceebada3.png)

## Data Source
The pipeline utilizes the Northwind sample database provided by Microsoft, which represents a fictional company called Northwind Traders.

## Pipeline Stages

### Bronze Stage
In this stage, raw data is ingested from AWS RDS using Airbyte and subsequently processed in Databricks. As the data is in JSON format, it must be parsed before being transformed into a structured format and stored in Delta tables. The following tables are selected for further processing:

customers
orders
orders_details

### Silver Stage
During the silver stage, the data from the bronze tables is cleaned, transformed, and prepared for dimensional modeling. Data quality checks are performed using the Great Expectations library, which helps ensure the accuracy and consistency of the data.

### Gold Stage
In the gold stage, advanced data processing and aggregation are performed to generate insights and facilitate reporting. The results can be visualized in dashboards, enabling management teams to make data-driven decisions. 

## Dimensional Model:

![Screenshot 2023-04-04 at 7 06 19 AM](https://user-images.githubusercontent.com/101911329/229883120-feedd709-a170-4352-ae5c-b3c88672e2e4.png)

### Data Quality and Validation

Data quality testing is performed using the Great Expectations library, which helps maintain the integrity of the data throughout the pipeline. Expectations are defined for each DataFrame, and the data is validated against these expectations to ensure its quality.

## Databricks Workflow:
![Screenshot 2023-04-03 at 11 13 37 PM](https://user-images.githubusercontent.com/101911329/229883294-2b871fbd-3ec4-40a8-80e3-39b139f9890d.png)

## Final Dashboard
The final dashboard has the following visualizations:

- Total Revenue by Month (line chart)
- Top 10 Customers by Revenue (bar chart)
- AVG Shipping cost by Shipping Company (3 line chart)
- AVG Shipping cost by Country (map)

![Screenshot 2023-04-03 at 11 07 38 PM](https://user-images.githubusercontent.com/101911329/229883426-923b9d5a-99da-48cf-a7ed-0a2d64cd07c1.png)



