# Numeric Attribute Correlation

## Introduction
Find correlations between the numeric attributes of any table. Only the quantitative variables are used  and only the connections between  these variables are of interest. Example: If the blue t shirts are selling good in February, does this mean that the red pants are also selling good in February. 

## Tasks
1. Find if there is a correlation between the numeric atributes in a table.

## Technologies used
MySQL Workbench
Python(mysql.connector and re libraries)

## Setup

1. Download the Data_Cortex_Nuclear.csv file from https://www.kaggle.com/ruslankl/mice-protein-expression.
2. Import 'Data_Cortex_Nuclear.csv' into MySQL.
3. Remove the non-numeric columns(MouseID, Genotype, Treatment, Behaviour and class).
4. Add an 'id AUTO_INCREMENT' to the table.
5. Open the numeric_attribute_correlation.py file and change the mysql.connector.connect data to your host, user and password data. Change the database name and the table name with the names you chose when creating the main object at line 287. 

## Examples of use

