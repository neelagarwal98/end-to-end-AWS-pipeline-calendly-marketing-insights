import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, explode, expr

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Load source
dyf = glueContext.create_dynamic_frame.from_catalog(
    database="calendly_project_db",
    table_name="scheduled_events_historical"
)

# Explode and flatten
df = dyf.toDF()
df_flat = df.selectExpr("explode(collection) as event") \
    .select(
        col("event.uri").alias("event_id"),
        col("event.name").alias("event_name"),
        col("event.status"),
        col("event.start_time"),
        col("event.end_time"),
        expr("timestampdiff(MINUTE, event.start_time, event.end_time)").alias("duration_minutes"),
        col("event.event_type").alias("event_type_uri"),
        col("event.created_at").alias("event_created_at"),
        col("event.location.type").alias("location_type"),
        col("event.location.location").alias("location_detail"),
        col("event.event_memberships")[0]["user_email"].alias("organizer_email"),
        col("event.event_memberships")[0]["user_name"].alias("organizer_name"),
        col("event.calendar_event.external_id").alias("calendar_event_id")
    )

# Write to Parquet
df_flat.write.mode("overwrite").parquet("s3://calendly-marketing-data/transformed/scheduled_events/")
