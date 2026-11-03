import os
from urllib.parse import urlparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType


spark = (
    SparkSession.builder
    .appName("Credit-Analysis")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .getOrCreate()
)


try:
    
    pg_user = os.getenv("PG_USER")
    pg_password = os.getenv("PG_PASSWORD")

    
    def config_s3(spark):
        endpoint = os.environ["MINIO_ENDPOINT"]
        parsed = urlparse(endpoint)

        hconf = spark.sparkContext._jsc.hadoopConfiguration()

        hconf.set("fs.s3a.endpoint", endpoint)
        hconf.set("fs.s3a.access.key", os.environ["MINIO_ACCESS_KEY"])
        hconf.set("fs.s3a.secret.key", os.environ["MINIO_SECRET_KEY"])
        hconf.set("fs.s3a.path.style.access", "true")
        hconf.set(
            "fs.s3a.connection.ssl.enabled",
            str(parsed.scheme == "https").lower()
        )
        hconf.set(
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )

    config_s3(spark)

    
    credit_df = (
        spark.read
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/credit_db")
        .option("dbtable", "public.credit_applications")
        .option("user", pg_user)
        .option("password", pg_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    
    blacklist_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("reason", StringType(), True)
    ])

    
    blacklist_df = (
        spark.read
        .option("header", True)
        .schema(blacklist_schema)
        .csv("/opt/airflow/data/blacklist.csv")
    )

    
    join_df = credit_df.join(
        blacklist_df,
        on="customer_id",
        how="left"
    )

    
    join_df = join_df.withColumn(
        "risk_status",
        F.when(
            F.col("reason").isNotNull(),
            "rejected"
        ).when(
            (F.col("requested_amount") > F.col("monthly_income") * 12) |
            (F.col("existing_debt") > F.col("monthly_income") * 0.7) |
            (F.col("employment_years") < 1) |
            (F.col("age") < 21),
            "high_risk"
        ).otherwise("approved")
    )

    join_df.groupBy("risk_status").count().show()

    (
        join_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("application_date")
        .save("s3a://credit-bucket/delta/credit_applications")
    )

    print("Success")

finally:
    spark.stop()
