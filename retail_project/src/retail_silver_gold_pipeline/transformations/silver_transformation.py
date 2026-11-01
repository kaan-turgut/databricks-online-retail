from pyspark.sql.functions import *
from pyspark import pipelines as dp

@dp.table(name="retail_project.silver.transactions")
@dp.expect("valid_invoice_date", "invoice_timestamp IS NOT NULL")
def transactions():
    return (
        spark.readStream.table("retail_project.bronze.transactions")
        .withColumn("customerID", col("CustomerID").cast("int"))
        .withColumn("total", col("Quantity") * col("Price"))
        .withColumn(
            "time_period",
            when(hour("InvoiceDate").between(5, 7), "Early Morning")
            .when(hour("InvoiceDate").between(8, 11), "Morning")
            .when(hour("InvoiceDate").between(12, 16), "Afternoon")
            .when(hour("InvoiceDate").between(17, 22), "Evening")
            .otherwise("Night")
        )
        .withColumn(
            "transaction_type",
            when(
                upper(col("Invoice")).startswith("C"),
                "Cancellation"
            )
            .when(
                (col("Quantity") < 0) & (col("Price") == 0),
                "Inventory Adjustment"
            )
            .when(
                (col("Quantity") > 0) & (col("Price") > 0),
                "Sale"
            )
            .otherwise("Other")
        )
        .select(
            col("Invoice").alias("invoice_id"),
            col("StockCode").alias("product_id"),
            col("Description").alias("description"),
            col("customerID").alias("customer_id"),
            col("InvoiceDate").alias("invoice_timestamp"),
            to_date("InvoiceDate").alias("invoice_date"),
            col("Country").alias("country"),
            col("Quantity").alias("quantity"),
            col("Price").alias("unit_price"),
            col("total").alias("line_amount"),
            col("transaction_type"),
            col("time_period"),
            col("source_file"),
            col("ingested_at")
        )
    )