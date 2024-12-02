import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1733168167347 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://udacity-deaws-lakes-nguyenmphu/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node1733168167347",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1733168201213 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://udacity-deaws-lakes-nguyenmphu/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1733168201213",
)

# Script generated for node Populate With Accelerometer Data
SqlQuery0 = """
select * from step_trainer_trusted s join accelerometer_trusted a on s.sensorreadingtime = a.timestamp
"""
PopulateWithAccelerometerData_node1733170271592 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "step_trainer_trusted": StepTrainerTrusted_node1733168167347,
        "accelerometer_trusted": AccelerometerTrusted_node1733168201213,
    },
    transformation_ctx="PopulateWithAccelerometerData_node1733170271592",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1733168588580 = glueContext.getSink(
    path="s3://udacity-deaws-lakes-nguyenmphu/machine_learning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_node1733168588580",
)
MachineLearningCurated_node1733168588580.setCatalogInfo(
    catalogDatabase="steadi", catalogTableName="machine_learning_curated"
)
MachineLearningCurated_node1733168588580.setFormat("glueparquet", compression="snappy")
MachineLearningCurated_node1733168588580.writeFrame(
    PopulateWithAccelerometerData_node1733170271592
)
job.commit()
