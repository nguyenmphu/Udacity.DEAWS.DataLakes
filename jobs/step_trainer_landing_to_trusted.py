import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Curated
CustomerCurated_node1733164789593 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://udacity-deaws-lakes-nguyenmphu/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1733164789593",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1733164727369 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiLine": "true"},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-deaws-lakes-nguyenmphu/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1733164727369",
)

# Script generated for node Select Serial Number
SelectSerialNumber_node1733166727470 = SelectFields.apply(
    frame=CustomerCurated_node1733164789593,
    paths=["serialNumber"],
    transformation_ctx="SelectSerialNumber_node1733166727470",
)

# Script generated for node Filter Customer
StepTrainerLanding_node1733164727369DF = StepTrainerLanding_node1733164727369.toDF()
SelectSerialNumber_node1733166727470DF = SelectSerialNumber_node1733166727470.toDF()
FilterCustomer_node1733166455493 = DynamicFrame.fromDF(
    StepTrainerLanding_node1733164727369DF.join(
        SelectSerialNumber_node1733166727470DF,
        (
            StepTrainerLanding_node1733164727369DF["serialnumber"]
            == SelectSerialNumber_node1733166727470DF["serialNumber"]
        ),
        "leftsemi",
    ),
    glueContext,
    "FilterCustomer_node1733166455493",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1733166882406 = glueContext.getSink(
    path="s3://udacity-deaws-lakes-nguyenmphu/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_node1733166882406",
)
StepTrainerTrusted_node1733166882406.setCatalogInfo(
    catalogDatabase="steadi", catalogTableName="step_trainer_trusted"
)
StepTrainerTrusted_node1733166882406.setFormat("glueparquet", compression="snappy")
StepTrainerTrusted_node1733166882406.writeFrame(FilterCustomer_node1733166455493)
job.commit()
