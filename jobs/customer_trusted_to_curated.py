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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1733162161194 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://udacity-deaws-lakes-nguyenmphu/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1733162161194",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1733162123513 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://udacity-deaws-lakes-nguyenmphu/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1733162123513",
)

# Script generated for node Filter Trusted Customer And Has Accelerommeter Data
FilterTrustedCustomerAndHasAccelerommeterData_node1733162205124 = Join.apply(
    frame1=CustomerTrusted_node1733162123513,
    frame2=AccelerometerTrusted_node1733162161194,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="FilterTrustedCustomerAndHasAccelerommeterData_node1733162205124",
)

# Script generated for node Drop Fields
DropFields_node1733162321748 = DropFields.apply(
    frame=FilterTrustedCustomerAndHasAccelerommeterData_node1733162205124,
    paths=[
        "z",
        "user",
        "y",
        "x",
        "email",
        "phone",
        "timestamp",
        "birthday",
        "customername",
    ],
    transformation_ctx="DropFields_node1733162321748",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1733162747889 = DynamicFrame.fromDF(
    DropFields_node1733162321748.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1733162747889",
)

# Script generated for node Customer Curated
CustomerCurated_node1733162377924 = glueContext.getSink(
    path="s3://udacity-deaws-lakes-nguyenmphu/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node1733162377924",
)
CustomerCurated_node1733162377924.setCatalogInfo(
    catalogDatabase="steadi", catalogTableName="customer_curated"
)
CustomerCurated_node1733162377924.setFormat("glueparquet", compression="snappy")
CustomerCurated_node1733162377924.writeFrame(DropDuplicates_node1733162747889)
job.commit()
