#!/usr/bin/env python
# coding: utf-8

# New notebook


## Dependancies

import logging
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import*
from pyspark.sql.functions import col, regexp_replace, trim, upper, to_date, months_between, date_format
from pyspark.sql.window import Window
from pyspark.sql.types import DateType
from pyspark.sql import functions as F

from pyspark.sql.functions import (
    col, sum as _sum, to_date, date_format, when, lit, broadcast, isnan, count, stddev, mean
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, IntegerType


## Logging Configuration

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("etl_pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("LoanRepaymentETL")


## Define Spark Session 

def create_spark_session(app_name="LoanRepaymentETL"):
    return (
        SparkSession.builder
        .appName(app_name)
        # Memory and cores
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.cores", "2")
        .config("spark.sql.shuffle.partitions", "200")  # Tune based on data size
        .config("spark.default.parallelism", "200")     # Tune based on cluster size

        # Optimizations
        .config("spark.sql.adaptive.enabled", "true")  # Enable Adaptive Query Execution
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", "10MB")  # Adjust for lookup table size

        # Delta Lake specific
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")

        .getOrCreate()
    )


## Define Schemas

transaction_schema = StructType([
    StructField("uuid", StringType(), True),
    StructField("repmt_date", StringType(), True),
    StructField("repmt_amt", DoubleType(), True)
])

account_schema = StructType([
    StructField("uuid", StringType(), True),
    StructField("activation_date", DateType(), True),
    StructField("total_cost", DoubleType(), True),
    StructField("product_name", StringType(), True),
    StructField("customer_gender", StringType(), True),
    StructField("long", DoubleType(), True),
    StructField("lat", DoubleType(), True),
    StructField("country", StringType(), True),
    StructField("follow_on_cost", DoubleType(), True),
    StructField("downpmt_cost", DoubleType(), True),
    StructField("contract_days", IntegerType(), True),
    StructField("follow_on_days", IntegerType(), True),
    StructField("receivable_per_day", DoubleType(), True),
    StructField("organization", StringType(), True),
    StructField("exchange_rate", DoubleType(), True),
    StructField("adm2", StringType(), True)  
])

## Define Paths

transaction_path="Files/data.csv"
accounts_path="Files/data.csv"
output_dir = "Files/data"

## Extraction Layer

def extract_data(spark, transaction_path, accounts_path):
    try:
        transaction_df = spark.read.csv(transaction_path, header=True, schema=transaction_schema)
        accounts_df = spark.read.csv(accounts_path, header=True, schema=account_schema)
        logger.info("Successfully extracted data.")
        return transaction_df, accounts_df
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise
  


#transaction_df, accounts_df = extract_data(spark, transaction_path, accounts_path)


#display(transaction_df)


## Validation Layer

def validate_data(accounts_df, name="DataFrame"):
    try:
        logger.info(f"Validating {name}")

        # Safe null + NaN checks
        null_nan_report = accounts_df.select([
            count(when(col(c).isNull() | (isnan(col(c)) if dtype in ['double', 'float'] else lit(False)), c)).alias(c)
            for c, dtype in accounts_df.dtypes
        ])
        null_nan_report.show(truncate=False)

        # Duplicate rows
        dup_count = accounts_df.count() - accounts_df.dropDuplicates().count()
        logger.info(f"Duplicate rows in {name}: {dup_count}")

        # Date-specific check (optional)
        if "repmt_date" in transaction_df.columns:
            invalid_dates = transaction_df.filter(col("repmt_date").isNull()).count()
            logger.info(f"Invalid or null repayment dates in {name}: {invalid_dates}")

        # Outlier detection in numeric columns
        for numeric_col in [f.name for f in accounts_df.schema.fields if isinstance(f.dataType, (DoubleType, FloatType))]:
            stats = accounts_df.select(mean(col(numeric_col)).alias("mean"), stddev(col(numeric_col)).alias("stddev")).first()
            avg, std_dev = stats["mean"], stats["stddev"] or 0
            if std_dev > 0:
                upper_bound = avg + 3 * std_dev
                lower_bound = avg - 3 * std_dev
                outlier_count = accounts_df.filter((col(numeric_col) > upper_bound) | (col(numeric_col) < lower_bound)).count()
                logger.info(f"Outliers in column '{numeric_col}' of {name}: {outlier_count}")
    except Exception as e:
        logger.error(f"Validation failed for {name}: {e}")
        raise


# null_nan_report,dup_count, invalid_dates, outlier_count  = validate_data(accounts_df, name="DataFrame")



## Transformation Layer

def transform_data(transaction_df, accounts_df):
    try:
        transaction_df = transaction_df.withColumn("repmt_date", to_date("repmt_date", "yyyy-MM-dd"))

        total_paid_df = transaction_df.groupBy("uuid").agg(_sum("repmt_amt").alias("total_paid"))

        accounts_df = broadcast(accounts_df)
        joined_df = total_paid_df.join(accounts_df, on="uuid", how="left")

        result_df = joined_df.withColumn(
            "percent_repaid",
            when(
                (col("total_cost").isNotNull()) & (col("total_cost") != 0),
                col("total_paid") / col("total_cost")
            ).otherwise(lit(None))
        )

        monthly_tx_df = transaction_df.withColumn("month", date_format("repmt_date", "yyyy-MM"))
        monthly_paid_df = monthly_tx_df.groupBy("uuid", "month").agg(_sum("repmt_amt").alias("monthly_paid"))
        monthly_joined_df = monthly_paid_df.join(accounts_df, on="uuid", how="left")
        monthly_result_df = monthly_joined_df.withColumn(
            "monthly_percent_repaid",
            when(
                (col("total_cost").isNotNull()) & (col("total_cost") != 0),
                col("monthly_paid") / col("total_cost")
            ).otherwise(lit(None))
        )
        # Sort result

        monthly_result_df = monthly_result_df.orderBy("uuid", "month")

        logger.info("Transformation successful.")
        return result_df, monthly_result_df
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        raise





#result_df, monthly_result_df= transform_data(transaction_df, accounts_df)


#display(result_df)
#display(monthly_result_df)


##  Loading Layer

def load_data(monthly_result_df, output_dir, label="monthly_repayment_rates"):
    try:
        timestamp = datetime.datetime.now().strftime("%Y%m%d")
        output_path = f"{output_dir}/{label}_{timestamp}.csv"
        monthly_result_df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")
        logger.info(f"Data written to: {output_path}")
    except Exception as e:
        logger.error(f"Load failed: {e}")
        raise



#output_path= load_data(monthly_result_df, output_dir, label="monthly_repayment_rates")

#display(monthly_result_df)


## Main ETL job 

def main():
    spark = create_spark_session()

    transaction_path="Files/Nithio/SunCompany_payments_data.csv"
    accounts_path="Files/Nithio/SunCompany_accounts_data.csv"
    output_dir = "Files/Nithio"

    try:
        transaction_df, accounts_df = extract_data(spark, transaction_path, accounts_path)

        validate_data(transaction_df, "Transactions")
        validate_data(accounts_df, "Accounts")

        result_df, monthly_result_df = transform_data(transaction_df, accounts_df)

        load_data(monthly_result_df, output_dir)

        logger.info("TransformationLoanRepaymentETL successful.")    
    except Exception as e:
        logger.critical(f"Pipeline execution failed: {e}")
    finally:
        spark.stop()
if __name__ == "__main__":
    main()

