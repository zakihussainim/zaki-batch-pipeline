import sys
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --- Job setup ---------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "RAW_PATH", "CURATED_PATH", "QUARANTINE_PATH", "SNS_TOPIC_ARN"]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

DQ_FAILURE_THRESHOLD = 0.10  # fail the job if more than 10% of records are bad

# --- Read raw data -------------------------------------------------------
raw_df = spark.read.csv(args["RAW_PATH"], header=True, inferSchema=True)

raw_df.printSchema()
raw_df.show(5)

# --- Data quality: flag missing fields and duplicates ---------------------
flagged_df = raw_df.withColumn(
    "dq_issue",
    F.when(F.col("drawing_number").isNull() | (F.trim(F.col("drawing_number")) == ""), "missing_drawing_number")
     .when(F.col("client_name").isNull() | (F.trim(F.col("client_name")) == ""), "missing_client_name")
     .otherwise(None)
)

window_spec = Window.partitionBy("drawing_number", "revision").orderBy(F.lit(1))
flagged_df = flagged_df.withColumn("dq_row_num", F.row_number().over(window_spec))

flagged_df = flagged_df.withColumn(
    "dq_issue",
    F.when(F.col("dq_issue").isNotNull(), F.col("dq_issue"))
     .when(F.col("dq_row_num") > 1, "duplicate_drawing_revision")
     .otherwise(None)
)

# --- Split into clean vs quarantined --------------------------------------
clean_df = flagged_df.filter(F.col("dq_issue").isNull()).drop("dq_issue", "dq_row_num")
quarantine_df = flagged_df.filter(F.col("dq_issue").isNotNull()).drop("dq_row_num")

total_count = flagged_df.count()
bad_count = quarantine_df.count()
bad_rate = bad_count / total_count if total_count > 0 else 0

print(f"Total records: {total_count}, quarantined: {bad_count}, bad rate: {bad_rate:.2%}")

# --- Write quarantined records + notify -----------------------------------
if bad_count > 0:
    quarantine_df.write.mode("overwrite").option("header", True).csv(args["QUARANTINE_PATH"])

    sns = boto3.client("sns")
    message = (
        f"Data quality check flagged {bad_count} of {total_count} records "
        f"({bad_rate:.2%}) in this pipeline run.\n"
        f"Quarantined records written to: {args['QUARANTINE_PATH']}\n"
        f"Please review."
    )
    sns.publish(
        TopicArn=args["SNS_TOPIC_ARN"],
        Subject="Drawing metadata pipeline - data quality issues found",
        Message=message
    )

# --- Fail the job if the bad-record rate is too high ----------------------
if bad_rate > DQ_FAILURE_THRESHOLD:
    job.commit()  # commit what we've done (quarantine write + notification) before failing
    raise Exception(
        f"Data quality check failed: {bad_rate:.2%} of records were bad "
        f"(threshold: {DQ_FAILURE_THRESHOLD:.0%}). See quarantine output and SNS alert for details."
    )

# --- Write clean data to curated zone, partitioned by date ----------------
clean_df = clean_df.withColumn("issued_year", F.year("issued_date")) \
                    .withColumn("issued_month", F.month("issued_date"))

clean_df.write.mode("overwrite") \
    .partitionBy("issued_year", "issued_month") \
    .parquet(args["CURATED_PATH"])

job.commit()