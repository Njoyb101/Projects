import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# Source & Target Variables
source_db = "nj-yta-db-raw"
source_tbl = "video_details"
target_bucket = "s3://nj-yta-clean-apsouth1-dev/YT_video_details_csv/"

# Predicate Pushdown to remove the  chars not  getting parsed due to utf-8 
predicate_pushdown = "region in ('ca','gb','us')"

datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database = source_db, 
    table_name = source_tbl, 
    transformation_ctx = "datasource0", 
    push_down_predicate = predicate_pushdown
)

applymapping1 = ApplyMapping.apply(
    frame = datasource0, 
    mappings = [
        ("video_id", "string", "video_id", "string"),
        ("trending_date", "string", "trending_date", "string"),
        ("title", "string", "title", "string"),
        ("channel_title", "string", "channel_title", "string"),
        ("category_id", "long", "category_id", "long"),
        ("publish_time", "string", "publish_time", "string"),
        ("tags", "string", "tags", "string"),
        ("views", "long", "views", "long"),
        ("likes", "long", "likes", "long"),
        ("dislikes", "long", "dislikes", "long"),
        ("comment_count", "long", "comment_count", "long"),
        ("thumbnail_link", "string", "thumbnail_link", "string"),
        ("comments_disabled", "boolean", "comments_disabled", "boolean"),
        ("ratings_disabled", "boolean", "ratings_disabled", "boolean"),
        ("video_error_or_removed", "boolean", "video_error_or_removed", "boolean"),
        ("description", "string", "description", "string"),
        ("region", "string", "region", "string")
    ], 
    transformation_ctx = "applymapping1"
)

resolvechoice2 = ResolveChoice.apply(
    frame = applymapping1, 
    choice = "make_struct", 
    transformation_ctx = "resolvechoice2"
)

dropnullfields3 = DropNullFields.apply(
    frame = resolvechoice2, 
    transformation_ctx = "dropnullfields3"
)


datasink1 = dropnullfields3.toDF().coalesce(1)

df_final_output = DynamicFrame.fromDF(datasink1, glueContext, 'df_final_output')

datasink4 = glueContext.write_dynamic_frame.from_options(
    frame = dropnullfields3, 
    connection_type = "s3", 
    connection_options = {
        "path": target_bucket, 
        "partitionKeys": ["region"]
        }, 
    format = "parquet", 
    format_options={"compression": "snappy"},
    transformation_ctx = "datasink4"
)

job.commit()