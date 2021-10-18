# covidUsa
Created a PySpark code that reads Covid dataset for USA from Blob Storage and the States full name from ADLS Gen 2 Storage.
The Join of these 2 datasets is further manipulated to create a final dataset which contains the count of positive cases per state in USA for Jan 2021 and Feb 2021.
With this data I have created a PowerBI report which shows the difference between Previous cases (Jan) and Current Cases(Feb) through an Area chart and a Map view.
Used:
Azure Data Factory- for Copy Data 
Azure Blob Storage
Azure Data Lake Storage Gen 2
Azure Databricks
Power BI
