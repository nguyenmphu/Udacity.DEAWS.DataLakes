import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing
CustomerLanding_node1733154526590 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiLine": "true"},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-deaws-lakes-nguyenmphu/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1733154526590",
)

# Script generated for node Filter Trusted Customer
FilterTrustedCustomer_node1733154568092 = Filter.apply(
    frame=CustomerLanding_node1733154526590,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="FilterTrustedCustomer_node1733154568092",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1733154619517 = glueContext.getSink(
    path="s3://udacity-deaws-lakes-nguyenmphu/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node1733154619517",
)
CustomerTrusted_node1733154619517.setCatalogInfo(
    catalogDatabase="steadi", catalogTableName="customer_trusted"
)
CustomerTrusted_node1733154619517.setFormat("glueparquet", compression="snappy")
CustomerTrusted_node1733154619517.writeFrame(FilterTrustedCustomer_node1733154568092)
job.commit()
