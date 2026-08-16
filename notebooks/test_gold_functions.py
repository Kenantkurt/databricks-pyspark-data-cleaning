import pytest
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual
from gold_functions import top_n_per_city

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()

def test_top_n_per_city(spark):
    input_df = spark.createDataFrame([
        ("A", "R1", 400.0),
        ("A", "R2", 300.0),
        ("A", "R3", 200.0),
        ("A", "R4", 200.0),
        ("A", "R5", 100.0),
        ("B", "R6", 80.0),
        ("B", "R7", 60.0),
    ], ["city", "restaurant_name", "revenue"])

    expected_df = spark.createDataFrame([
        ("A", "R1", 400.0),
        ("A", "R2", 300.0),
        ("A", "R3", 200.0),
        ("B", "R6", 80.0),
        ("B", "R7", 60.0),
    ], ["city", "restaurant_name", "revenue"])

    result_df = top_n_per_city(input_df)
    assertDataFrameEqual(result_df, expected_df)



def test_small_group(spark):
    input_df = spark.createDataFrame([
        ("B", "R6", 80.0),
        ("B", "R7", 60.0),
    ], ["city", "restaurant_name", "revenue"])

    result_df = top_n_per_city(input_df, n=3)

    assert result_df.count() == 2
