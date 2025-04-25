## @type: job

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Leer una tabla real (aunque no la uses)
dyf_dummy = glueContext.create_dynamic_frame.from_catalog(
    database="db_landing",
    table_name="ld_teams"
)

job.commit()