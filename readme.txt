Python HYBRIDJOIN Implementation + MySQL Star Schema
1. Introduction
This ReadMe provides the exact steps required to run the my Data Warehouse project.
It explains how to set up the database, run the Python HYBRIDJOIN program, and execute the OLAP queries.
Follow each instruction in sequence to ensure correct execution and successful data loading.
2. Software Requirements
Before running the project, install the following:
1. Python 3.8 or higher

2. MySQL Server (5.7 or 8.0 recommended)

3. MySQL Workbench

4. Required Python libraries

Install libraries using:
pip install pandas numpy sqlalchemy pymysql


Ensure all provided CSV and Excel files are placed in the same directory as the Python script.


3. Files Included in the Project
The project folder must contain the following files:
   1. .sql

   2. .py

   3. customer_master_data.xlsx

   4. product_master_data.xlsx

   5. transactional_data.csv

   6. Project Report (Word Document)

   7. ReadMe.txt (this file)



4. Step-by-Step Instructions
STEP 1 – Create the Database and Star Schema
      1. Open MySQL Workbench.

      2. Connect to your MySQL server.

      3. Open the file Ziyan_22i1998_SQL.sql.

      4. Execute the script.

This script will:
         * Drop any existing schema, tables, or views.

         * Create all dimension tables.

         * Create the fact_sales table (raw).

         * Create fact_sales_enriched table.

         * Create the STORE_QUARTERLY_SALES view.

If no errors appear, the Data Warehouse schema is ready.


STEP 2 – Run the Python HYBRIDJOIN ETL Program
            1. Open a terminal or command prompt.

            2. Navigate to the folder containing Ziyan_22I1998_pythonscript.py.

            3. Run this
               4. When the program starts, it will:

                  * Ask for database credentials.

                  * Extract data from all input files.

                  * Clean and transform customer, product, and date data.

                  * Load all dimension tables into MySQL.

                  * Insert raw transactional rows into fact_sales.

                  * Start the streaming thread to simulate near-real-time incoming transactions.

                  * Run HYBRIDJOIN to enrich transactional data with master data.

                  * Continuously insert enriched records into fact_sales_enriched.

                     5. Wait until the console displays:

HYBRIDJOIN COMPLETED


This confirms that all transactions have been fully processed.


STEP 3 – Verify the Loaded Data
Open MySQL Workbench and run:
SELECT COUNT(*) FROM fact_sales_enriched;


If the count is greater than zero, enrichment was successful.
You can also preview the joined data:
SELECT * FROM fact_sales_enriched LIMIT 20;




STEP 4 – Run the OLAP Analytical Queries
                        1. Open 22i1998_Ziyan_SQL.sql in MySQL Workbench.

                        2. Navigate to the queries section Execute the entire script or individual queries.

                        3. These queries will provide:

                           1. Revenue trends

                           2. Weekend vs weekday performance

                           3. Top products

                           4. Category and occupation analysis

                           5. Store and supplier performance

                           6. Seasonal insights

                           7. Outlier detection

                           8. Monthly and quarterly drill-downs

If fact_sales_enriched is correctly populated, all queries will run without errors.
5. Troubleshooting
                              1. If MySQL rejects inserts due to foreign keys, ensure .sql file was executed completely.

                              2. If Python cannot connect to MySQL, verify that:

                                 1. MySQL service is running

                                 2. Correct username and password were entered

                                 3. Port 3306 is open

                                    3. If fact_sales_enriched is empty, rerun .py after confirming that:

                                       1. All CSV/Excel files are in the same directory

                                       2. Their filenames match exactly

                                          4. If Python throws a missing library error, reinstall dependencies.



6. Completion Confirmation
The project is successfully executed when all of the following are true:
                                             * All dimension tables contain data

                                             * fact_sales contains raw transactional rows

                                             * fact_sales_enriched contains enriched records

                                             * STORE_QUARTERLY_SALES view returns aggregated results

                                             * All OLAP queries run without errors

At this point, the Data Warehouse is fully operational.
