# Data Pipeline for Customer Segmentation of Lemon Farm Cafe
The purpose of this project is to develop an ETL Pipeline aimed at efficiently processing and transforming data from Lemon Farm Cafe, 
which is stored in Amazon S3. The processed data will be loaded into a PostgreSQL database for effective data management and analysis, 
with Prefect serving as the orchestrator. Additionally, the data in the database will be leveraged to create a dashboard using Looker Studio, 
enabling analysis and the discovery of customer insights. Furthermore, a machine learning model will be utilized for cluster analysis in customer 
segmentation, employing the k-means clustering model.

![Alt text](img/overview.png?raw=true "Title")

## Example Data
- Raw data
  
![Alt text](img/Ex_data.png?raw=true "Title")

- Transformed data

![Alt text](img/Transform_data.png?raw=true "Title")

## Dashboard

The Lemon Farm Cafe dashboard developed by Looker Studio comprises three pages, each spanning a 14-day period. The first page provides an overview of various metrics such as Total Revenues, Daily Revenues, Total Orders, Daily Orders, Total Members, and Daily Members. It also includes a time series graph displaying revenue trends over the 14 days and a graphical representation of the relationship between revenues and order amounts categorized by menu items.

![Alt text](img/dashboard1.png?raw=true "Title")

The second page focuses on Customer Characteristics, featuring distributions of customer age and examining the relationship between revenues and orders based on gender and location.

![Alt text](img/dashboard2.png?raw=true "Title")


The third page, Time & Period, explores the relationship between revenues and order amounts across different days of the week (Sunday, Monday, etc.) and time periods.

![Alt text](img/dashboard3.png?raw=true "Title")

Dashboard link: https://lookerstudio.google.com/reporting/a934215a-0663-4451-9ba5-38b959314224

## Customer Segmentation
The project employed a machine learning model for cluster analysis, specifically utilizing the k-means clustering algorithm. Three experiments were conducted:

1. Menu Categories Clustering: This experiment focused on clustering customers based on their preferences for menu categories.

2. Menu Categories and Time of Day Clustering: This experiment expanded upon the first by considering both menu categories and the time of day, aiming to identify patterns in customer behavior related to specific menu items at different times.

3. Customer Characteristics Clustering: This experiment involved clustering customers based on various characteristics such as age, gender, location, or other relevant attributes to uncover distinct customer segments based on these characteristics.

![Alt text](img/experiment.png?raw=true "Title")

## Files in this project
- [LemonFarm_Presentation.pdf](LemonFarm_Presentation.pdf) -> Presentation file: Describe overview of project,data, methodologies, experiment, result, summary
- [etl_job.py](etl_job.py) -> Script for ETL job
- [customer_segmentation.ipynb](customer_segmentation.ipynb) -> Notebook file of customer_segmentation experiment
- [kmeans_numpy_improved.py](kmeans_numpy_improved.py) -> script for kmeans model
- [data_source folder](data_source) -> folder of source data consist of customer_data, product_categories, transaction


