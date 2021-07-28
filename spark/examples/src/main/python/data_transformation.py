# from pyspark.sql import SparkSession
# # from pyspark import SparkConf
# # import ibmos2spark
#
#
# def ss_builder(stocator=None):
#
#     # config = SparkConf().set("spark.jars", stocator)
#     spark = SparkSession.builder \
#         .appName("data_transformation") \
#         .getOrCreate()
#     return spark


# def schema_load(spark, creds):
#
#     schema = spark.read.format("jdbc").option("user", creds["DB2_USER"]).option("password", creds["DB2_PASSWORD"]) \
#         .option("driver", "com.ibm.db2.jcc.DB2Driver") \
#         .option("url",  creds["DB2_URL"]) \
#         .option("dbtable", creds["DB2_TABLE_NAME"]) \
#         .load()
#     return schema


# def calculating_amount_per_year(schema):
#
#     cols_to_agg = schema.columns[3:]
#     schema = schema.withColumn("year_purchases", sum(schema[col] for col in cols_to_agg)).drop(*cols_to_agg)
#     return schema


# def file_load_to_cos(spark, creds, schema):
#
#     credentials = {
#         "endpoint": creds["COS_ENDPOINT"],
#         "access_key": creds["COS_ACCESS_KEY"],
#         "secret_key": creds["COS_SECRET_KEY"]
#     }
#     cos = ibmos2spark.CloudObjectStorage(spark, credentials, "os_configs")
#     schema.write.csv(cos.url(creds["COS_DF_NAME"], creds["COS_BUCKET_NAME"]))


# if __name__ == "__main__":
#     creds_dict = {
#         "DB2_USER": "nqf40305",
#         "DB2_PASSWORD": "nlk5vv0xl-zcg7wb",
#         "DB2_URL": "jdbc:db2://dashdb-txn-sbox-yp-dal09-08.services.dal.bluemix.net:50000/BLUDB",
#         "DB2_TABLE_NAME": "year_sales",
#         "STCTR_JAR": r"C:\Users\Zaprudskaya_AA\stocator\target\stocator-1.1.4-SNAPSHOT-jar-with-dependencies.jar",
#         "COS_ENDPOINT": "s3.eu-de.cloud-object-storage.appdomain.cloud",
#         "COS_ACCESS_KEY": "fa1241eeb41746d38cb71331846da12a",
#         "COS_SECRET_KEY": "2f885f71f4389313c91c7b3ff8047dd1a5fc4c1a07f9b6ea",
#         "COS_BUCKET_NAME": "3my7bucket3",
#         "COS_DF_NAME": "year_purchases.csv"
#     }
#     ss = ss_builder(creds_dict["STCTR_JAR"])
#     print(ss)
    # dataframe = schema_load(ss, creds_dict)
    # mdf_df = calculating_amount_per_year(dataframe)
    # file_load_to_cos(ss, creds_dict, mdf_df)
    print('DONE')