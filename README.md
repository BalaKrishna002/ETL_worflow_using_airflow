# ETL_worflow_using_airflow

## SetUp and Running Locally
* Download & Install Docker
* Download yaml file for installation of Airflow
* Start Docker Engine
* Create a folder called airflow(as you wish) on the desktop, make sure to move yaml file into a folder
* enter `docker-compose up -d` with created folder

## Project Statement
An international firm that is looking to expand its business in different countries across the world has recruited you. You have been hired as a junior Data Engineer. You are tasked with creating an automated script that can extract the list of all countries in order of their GDPs in billion USDs (rounded to 2 decimal places), as logged by the International Monetary Fund (IMF). Since the IMF releases this evaluation twice a year, the organisation will use this code to extract the information as it is updated.

You can find the required data on this [Webpage](https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29).

The required information needs to be made accessible as a table 'Countries_by_GDP' in a database 'World_Economies' with attributes 'Country' and 'GDP_USD_billion.'

Your boss wants you to demonstrate the success of this code by running a query on the database table to display only the entries with more than a 100 billion USD economy. 

## ETL Workflow
Extract data by scraping from website ----> Transform the data -----> Load into PostgreSQL DB
