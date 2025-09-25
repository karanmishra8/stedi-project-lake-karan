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
step_landing_node1758776712688 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="step_trainer_landing", transformation_ctx="step_landing_node1758776712688")

# Script generated for node customer_curated
customer_curated_node1758776713599 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="customer_curated", transformation_ctx="customer_curated_node1758776713599")

# Script generated for node SQL Query
SqlQuery0 = '''
select s.* from s join c on c.serialnumber = s.SerialNumber
'''
SQLQuery_node1758776752908 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"c":customer_curated_node1758776713599, "s":step_landing_node1758776712688}, transformation_ctx = "SQLQuery_node1758776752908")

# Script generated for node step_trainer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758776752908, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758776707147", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
step_trainer_trusted_node1758776917449 = glueContext.getSink(path="s3://stedi-lake-house-project-karan/step_trainer/step_trainer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1758776917449")
step_trainer_trusted_node1758776917449.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1758776917449.setFormat("json")
step_trainer_trusted_node1758776917449.writeFrame(SQLQuery_node1758776752908)
job.commit()