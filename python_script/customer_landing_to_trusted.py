import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

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

# Script generated for node Amazon S3
AmazonS3_node1758783064408 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-project-karan/cutomer/cutomer_trusted/"], "recurse": True}, transformation_ctx="AmazonS3_node1758783064408")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from myDataSource where 	
sharewithresearchasofdate is not null
'''
SQLQuery_node1758775285360 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":AmazonS3_node1758783064408}, transformation_ctx = "SQLQuery_node1758775285360")

# Script generated for node customer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758775285360, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758775237929", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_trusted_node1758775386951 = glueContext.getSink(path="s3://stedi-lake-house-project-karan/cutomer/cutomer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_trusted_node1758775386951")
customer_trusted_node1758775386951.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="cutomer_trusted")
customer_trusted_node1758775386951.setFormat("json")
customer_trusted_node1758775386951.writeFrame(SQLQuery_node1758775285360)
job.commit()