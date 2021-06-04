import data_transformation as test_data
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality


spark = SparkSession.builder.getOrCreate()


# def schema_load_test(row_number):
#     schema = test_data.schema_load(spark, test_data.creds_dict)
#     df_row_count = schema.count()
#     assert df_row_count == row_number


def calculating_amount_test():
    df = spark.createDataFrame(
        [
            (1, 4, 2019, 5, 6, 1),
            (6, 9, 1993, 7, 6, 1),
            (5, 9, 2014, 6, 6, 1),
            (1, 3, 1978, 58, 6, 1),
        ],
        ["id", "pr_id", "year", "ms_1", "ms_2", "ms_3"]
    )
    test_df = test_data.calculating_amount_per_year(df)
    control_df = spark.createDataFrame(
        [
            (1, 4, 2019, 12),
            (5, 9, 2014, 13),
            (6, 9, 1993, 14),
            (1, 3, 1978, 65),
        ],
        ["id", "pr_id", "year", "year_purchases"]
    )
    assert_df_equality(test_df, control_df, ignore_row_order=True)

# schema_load_test(20000)
calculating_amount_test()
