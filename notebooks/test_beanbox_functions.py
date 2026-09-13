import pytest
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual
from beanbox_functions import revenue_per_category

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()

def test_revenue_per_category(spark):
    input_df = spark.createDataFrame(
        [
            ('Espresso', 1 , 50.0),
            ('Espresso', 2, 50.0),
            ('Lungo', 1, 20.0),
            ('Lungo', 2, 20.0),
            ('Decaf', 1, 30.0),
            ('Decaf', 2, 30.0)
        ],["category","qty","unit_price"]
    )

    expected_df = spark.createDataFrame(
        [
            ('Espresso',150.0),
            ('Lungo',60.0),
            ('Decaf',90.0)
        ],"category string, total_revenue double"
    )

    result_df = revenue_per_category(input_df)
    assertDataFrameEqual(result_df, expected_df)

    