import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node accelerometer_landing
accelerometer_landing_node1758775678663 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="accelerometer_landing", transformation_ctx="accelerometer_landing_node1758775678663")

# Script generated for node customer_trusted
customer_trusted_node1758775679391 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="cutomer_trusted", transformation_ctx="customer_trusted_node1758775679391")

# Script generated for node Join
Join_node1758775736749 = Join.apply(frame1=customer_trusted_node1758775679391, frame2=accelerometer_landing_node1758775678663, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1758775736749")

# Script generated for node SQL Query
SqlQuery0 = '''
select user , timestamp , x,y,z from myDataSource
'''
SQLQuery_node1758775765072 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1758775736749}, transformation_ctx = "SQLQuery_node1758775765072")

# Script generated for node Drop Duplicates
DropDuplicates_node1758775835006 =  DynamicFrame.fromDF(SQLQuery_node1758775765072.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1758775835006")

# Script generated for node accelerometer_trusted
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1758775835006, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758775237929", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
accelerometer_trusted_node1758775844196 = glueContext.getSink(path="s3://stedi-lake-house-project-karan/accelerometer/accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometer_trusted_node1758775844196")
accelerometer_trusted_node1758775844196.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="accelerometer_trusted")
accelerometer_trusted_node1758775844196.setFormat("json")
accelerometer_trusted_node1758775844196.writeFrame(DropDuplicates_node1758775835006)
job.commit()