import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.functions import col, to_timestamp

# Glue boilerplate
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load from Glue Data Catalog (crawler output)
dyf = glueContext.create_dynamic_frame.from_catalog(
    database="calendly_project_db",
    table_name="webhook_invitee_events_real_time"  # created by crawler on raw/real-time/
)

# Convert to Spark DataFrame
df = dyf.toDF()

# Flatten and transform
df_flat = df.select(
    col("event").alias("webhook_event"),
    to_timestamp("created_at").alias("webhook_created_at"),
    col("payload.email").alias("invitee_email"),
    col("payload.name").alias("invitee_name"),
    to_timestamp("payload.created_at").alias("invitee_created_at"),
    col("payload.status").alias("invitee_status"),
    col("payload.rescheduled"),
    col("payload.timezone"),
    col("payload.scheduled_event.uri").alias("event_id"),
    col("payload.scheduled_event.name").alias("event_name"),
    to_timestamp("payload.scheduled_event.start_time").alias("start_time"),
    to_timestamp("payload.scheduled_event.end_time").alias("end_time"),
    col("payload.scheduled_event.event_type").alias("event_type_uri"),
    col("payload.scheduled_event.event_memberships")[0]["user_email"].alias("organizer_email"),
    col("payload.scheduled_event.event_memberships")[0]["user_name"].alias("organizer_name")
)

# Write as Parquet
df_flat.write.mode("overwrite").parquet("s3://calendly-marketing-data/transformed/webhook_invitee_events/")

job.commit()
