import pytest
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual
from hotel_functions import clean_price


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()


def test_clean_price(spark):
    input_df = spark.createDataFrame([
        ("B1", "€ 320.50"),
        ("B2", "N/A"),
        ("B3", "410.00"),
    ], ["booking_id", "total_price"])

    # expected written BY HAND -- never copied from the function's own output
    expected_df = spark.createDataFrame([
        ("B1", 320.50),
        ("B2", None),
        ("B3", 410.00),
    ], ["booking_id", "total_price"])

    result_df = clean_price(input_df)
    assertDataFrameEqual(result_df, expected_df)
