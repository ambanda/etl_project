# etl_project
Airflow pipeline
Loan Repayment ETL Pipeline
This project builds a scalable, production-grade ETL pipeline using PySpark to analyze loan repayment performance from raw transaction and account data. It follows modular design principles, includes validation, transformation, and output stages, and is orchestrated for monthly execution (e.g., via Apache Airflow).
________________________________________
Project Structure
├── pipeline.py               # Main ETL script (Extraction, Validation, Transformation, Load)
├── dags/
│   └── loan_repayment_etl_dag.py # (Optional) Airflow DAG for scheduling
├── data/
│   ├── SunCompany_accounts_data.csv
│   └── SunCompany_payments_data.csv
├── output/
│   └── monthly_repayment_rates_<YYYYMMDD>.csv
├── README.md
└── requirements.txt          # (Optional) Environment setup
________________________________________
 How to Run
 1. Prerequisites
•	Python 3.8+
•	Apache Spark (tested with 3.x)
•	Optional: Airflow for orchestration
 2. Install Dependencies
pip install pyspark
If using Airflow:
pip install apache-airflow
3. Run the ETL Pipeline
spark-submit pipeline.py
The output CSV will be saved to the output/ directory.
________________________________________

 ETL Pipeline Overview
1. Extraction
Reads two CSVs:
•	accounts: Contains account metadata (loan amounts, dates, etc.)
•	transactions: Payment records (dates, amounts)
2. Validation
•	Null and missing value checks
•	Duplicate detection
•	Basic outlier detection using standard deviation
•	Logging of invalid records
3. Transformation
•	Aggregate payments per customer
•	Compute repayment percent vs total cost
•	Generate monthly repayment rates
•	Join with account data
4. Load
•	Outputs partitioned data to output/
•	Includes a timestamp in filename for versioning
________________________________________
 Performance & Scalability
This pipeline follows PySpark best practices for large-scale data:
•	 Explicit schemas avoid costly inference
•	Lazy transformations minimize intermediate shuffles
•	 Broadcast joins used for small dimension datasets (e.g., accounts)
•	Column pruning and caching where appropriate
•	Designed to scale to 10M+ rows with:
o	Partitioning by month or region
o	Adaptive Query Execution (Spark 3+)
o	Avoiding wide transformations like groupByKey
