from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.materialized_view(
    name="retail_project.gold.dim_customer"
)
def dim_customer():
    return (
        spark.read.table("retail_project.silver.transactions")
        .filter(col("customer_id").isNotNull())
        .groupBy("customer_id")
        .agg(
            min("invoice_timestamp").alias("first_transaction"),
            max("invoice_timestamp").alias("last_transaction"),
            countDistinct("invoice_id").alias("invoice_count"),
            count("*").alias("transaction_lines"),
            sum(
                when(
                    col("transaction_type") == "Sale",
                    col("line_amount")
                ).otherwise(0)
            ).alias("gross_sales_value"),
            sum("line_amount").alias("net_recorded_value")
        )
    )


@dp.materialized_view(
    name="retail_project.gold.daily_revenue"
)
def daily_revenue():
    return (
        spark.read.table("retail_project.silver.transactions")
        .groupBy("invoice_date")
        .agg(
            sum(
                when(
                    col("transaction_type") == "Sale",
                    col("line_amount")
                ).otherwise(0)
            ).alias("gross_sales"),

            sum(
                when(
                    col("transaction_type") == "Cancellation",
                    col("line_amount")
                ).otherwise(0)
            ).alias("cancellation_value"),

            sum("line_amount").alias("net_recorded_value"),

            countDistinct("invoice_id").alias("invoice_count"),

            count("*").alias("transaction_lines")
        )
    )


@dp.materialized_view(
    name="retail_project.gold.country_revenue"
)
def country_revenue():
    return (
        spark.read.table("retail_project.silver.transactions")
        .groupBy("country")
        .agg(
            sum(
                when(
                    col("transaction_type") == "Sale",
                    col("line_amount")
                ).otherwise(0)
            ).alias("gross_sales"),

            sum("line_amount").alias("net_recorded_value"),

            countDistinct(
                when(
                    col("transaction_type") == "Sale",
                    col("invoice_id")
                )
            ).alias("sales_invoice_count"),

            countDistinct("customer_id").alias("unique_customers")
        )
    )