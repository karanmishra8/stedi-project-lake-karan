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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1758777192500 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1758777192500")

# Script generated for node accelerometer_trsuted
accelerometer_trsuted_node1758777191679 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trsuted_node1758777191679")

# Script generated for node SQL Query
SqlQuery0 = '''
select serialnumber , x,y,z,a.timestamp from s
join a on s.sensorreadingtime = a.timestamp

'''
SQLQuery_node1758777283185 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"s":step_trainer_trusted_node1758777192500, "a":accelerometer_trsuted_node1758777191679}, transformation_ctx = "SQLQuery_node1758777283185")

# Script generated for node machine_learning_curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758777283185, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758776707147", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
machine_learning_curated_node1758777420874 = glueContext.getSink(path="s3://stedi-lake-house-project-karan/step_trainer/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1758777420874")
machine_learning_curated_node1758777420874.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="machine_learning_curated")
machine_learning_curated_node1758777420874.setFormat("json")
machine_learning_curated_node1758777420874.writeFrame(SQLQuery_node1758777283185)
job.commit()