import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.functions import to_date, col, row_number
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load new raw marketing spend data
dyf = glueContext.create_dynamic_frame.from_catalog(
    database="calendly_project_db",
    table_name="marketing_spend_marketing"
)
df_new = dyf.toDF().select(
    to_date(col("date")).alias("spend_date"),
    col("channel"),
    col("spend").cast("double")
)

# Try loading existing transformed data
try:
    df_existing = spark.read.parquet("s3://calendly-marketing-data/transformed/marketing_spend/")
    df_combined = df_existing.unionByName(df_new)
except AnalysisException:
    # If no existing data, only use new
    df_combined = df_new

# Deduplicate by spend_date and channel (keep last added row)
window_spec = Window.partitionBy("spend_date", "channel").orderBy(col("spend_date").desc())
df_deduped = df_combined.withColumn("row_num", row_number().over(window_spec)) \
                        .filter(col("row_num") == 1) \
                        .drop("row_num")

# Overwrite output with clean deduplicated data
df_deduped.write.mode("overwrite").parquet("s3://calendly-marketing-data/transformed/marketing_spend/")

job.commit()

