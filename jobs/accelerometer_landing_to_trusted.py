import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1733161026624 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiLine": "true"},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-deaws-lakes-nguyenmphu/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1733161026624",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1733161063926 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://udacity-deaws-lakes-nguyenmphu/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1733161063926",
)

# Script generated for node Filter Customer
FilterCustomer_node1733161089232 = Join.apply(
    frame1=CustomerTrusted_node1733161063926,
    frame2=AccelerometerLanding_node1733161026624,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="FilterCustomer_node1733161089232",
)

# Script generated for node Drop Fields
DropFields_node1733161239176 = DropFields.apply(
    frame=FilterCustomer_node1733161089232,
    paths=[
        "serialNumber",
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "shareWithFriendsAsOfDate",
        "lastUpdateDate",
        "phone",
        "email",
    ],
    transformation_ctx="DropFields_node1733161239176",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1733161270483 = DynamicFrame.fromDF(
    DropFields_node1733161239176.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1733161270483",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1733161289299 = glueContext.getSink(
    path="s3://udacity-deaws-lakes-nguyenmphu/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node1733161289299",
)
AccelerometerTrusted_node1733161289299.setCatalogInfo(
    catalogDatabase="steadi", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node1733161289299.setFormat("glueparquet", compression="snappy")
AccelerometerTrusted_node1733161289299.writeFrame(DropDuplicates_node1733161270483)
job.commit()
