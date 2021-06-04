from pyspark.sql import SparkSession
from pyspark import SparkConf
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

def ss_builder(stocator=None):

    config = SparkConf().set("spark.jars", stocator)
    spark = SparkSession.builder \
        .config(conf=config) \
        .appName("data_transformation") \
        .getOrCreate()
    return spark


def schema_load(spark, creds):

    schema = spark.read.format("jdbc").option("user", creds["DB2_USER"]).option("password", creds["DB2_PASSWORD"]) \
        .option("driver", "com.ibm.db2.jcc.DB2Driver") \
        .option("url",  creds["DB2_URL"]) \
        .option("dbtable", creds["DB2_TABLE_NAME"]) \
        .load()
    return schema


def calculating_amount_per_year(schema):

    cols_to_agg = schema.columns[3:]
    schema = schema.withColumn("year_purchases", sum(schema[col] for col in cols_to_agg)).drop(*cols_to_agg)
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
    ss = ss_builder(creds_dict["STCTR_JAR"])
    dataframe = schema_load(ss, creds_dict)
    mdf_df = calculating_amount_per_year(dataframe)
    file_load_to_cos(ss, creds_dict, mdf_df)
    print('DONE')