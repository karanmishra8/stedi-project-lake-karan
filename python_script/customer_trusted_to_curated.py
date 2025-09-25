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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1758776096171 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1758776096171")

# Script generated for node customer_trusted
customer_trusted_node1758776095451 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="cutomer_trusted", transformation_ctx="customer_trusted_node1758776095451")

# Script generated for node Join
Join_node1758776172724 = Join.apply(frame1=accelerometer_trusted_node1758776096171, frame2=customer_trusted_node1758776095451, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1758776172724")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct 
customername ,
email , 
phone , 
birthday, 
serialnumber , 
registrationdate,
lastupdatedate,
sharewithresearchasofdate,
sharewithpublicasofdate,
sharewithfriendsasofdate from myDataSource


'''
SQLQuery_node1758776190991 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1758776172724}, transformation_ctx = "SQLQuery_node1758776190991")

# Script generated for node Drop Duplicates
DropDuplicates_node1758776425577 =  DynamicFrame.fromDF(SQLQuery_node1758776190991.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1758776425577")

# Script generated for node customer_curated
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1758776425577, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758776089374", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_curated_node1758776432869 = glueContext.getSink(path="s3://stedi-lake-house-project-karan/cutomer/customer_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_curated_node1758776432869")
customer_curated_node1758776432869.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="customer_curated")
customer_curated_node1758776432869.setFormat("json")
customer_curated_node1758776432869.writeFrame(DropDuplicates_node1758776425577)
job.commit()