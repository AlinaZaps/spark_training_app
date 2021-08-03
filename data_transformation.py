from pyspark.sql import SparkSession
import time
import ibmos2spark
import os

creds_dict = {
    "DB2_USER": os.environ['DB2_USER'],
    "DB2_PASSWORD": os.environ['DB2_PASSWORD'],
    "DB2_URL": os.environ['DB2_URL'],
    "DB2_TABLE_NAME": os.environ['DB2_TABLE_NAME'],
    "COS_ENDPOINT": os.environ['COS_ENDPOINT'],
    "COS_ACCESS_KEY": os.environ['COS_ACCESS_KEY'],
    "COS_SECRET_KEY": os.environ['COS_SECRET_KEY'],
    "COS_BUCKET_NAME": os.environ['COS_BUCKET_NAME'],
    "COS_DF_NAME": os.environ['COS_DF_NAME']
}


def ss_builder():
    spark = SparkSession.builder \
        .appName("data_transformation") \
        .getOrCreate()
    return spark


def schema_load(spark, creds):
    schema = spark.read.format("jdbc").option("user", creds["DB2_USER"]).option("password", creds["DB2_PASSWORD"]) \
        .option("driver", "com.ibm.db2.jcc.DB2Driver") \
        .option("url", creds["DB2_URL"]) \
        .option("dbtable", "year_sales") \
        .option("numPartitions", 10) \
        .option("partitionColumn", "year") \
        .option("lowerBound", 1901) \
        .option("upperBound", 2021) \
        .load()
    return schema


def file_load_to_cos(spark, creds, schema):
    credentials = {
        "endpoint": creds["COS_ENDPOINT"],
        "access_key": creds["COS_ACCESS_KEY"],
        "secret_key": creds["COS_SECRET_KEY"]
    }
    cos = ibmos2spark.CloudObjectStorage(spark, credentials, "os_configs")
    schema.write.csv(cos.url(creds["COS_DF_NAME"], creds["COS_BUCKET_NAME"]))


if __name__ == "__main__":
    print(creds_dict)
    start = time.time()
    ss = ss_builder()
    mdf_df = schema_load(ss, creds_dict)
    file_load_to_cos(ss, creds_dict, mdf_df)
    end = time.time()
    print('TASK DURATION is ' + str(end - start))
    ss.stop()