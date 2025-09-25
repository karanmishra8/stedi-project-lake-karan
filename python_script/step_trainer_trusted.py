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

# Script generated for node step_landing
step_landing_node1758783518614 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-project-karan/step_trainer/step_trainer_landing/"], "recurse": True}, transformation_ctx="step_landing_node1758783518614")

# Script generated for node cus_curated
cus_curated_node1758783519199 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-project-karan/cutomer/customer_curated/"], "recurse": True}, transformation_ctx="cus_curated_node1758783519199")

# Script generated for node SQL Query
SqlQuery0 = '''
select s.* from s join c on c.serialnumber = s.SerialNumber
'''
SQLQuery_node1758776752908 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"s":step_landing_node1758783518614, "c":cus_curated_node1758783519199}, transformation_ctx = "SQLQuery_node1758776752908")

# Script generated for node step_trainer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758776752908, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758776707147", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
step_trainer_trusted_node1758776917449 = glueContext.getSink(path="s3://stedi-lake-house-project-karan/step_trainer/step_trainer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1758776917449")
step_trainer_trusted_node1758776917449.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1758776917449.setFormat("json")
step_trainer_trusted_node1758776917449.writeFrame(SQLQuery_node1758776752908)
job.commit()