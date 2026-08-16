import pytest
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual
from cinema_functions import top_movies_per_city


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()


def test_top_movies_per_city(spark):
    # Bravo and Charlie tie on 300 on purpose - that is what this test pins down.
    input_df = spark.createDataFrame(
        [
            ("Zwolle", "Alpha", 500.0),
            ("Zwolle", "Bravo", 300.0),
            ("Zwolle", "Charlie", 300.0),
            ("Zwolle", "Delta", 100.0),
            ("Assen", "Echo", 90.0),
        ],
        ["city", "movie_title", "price_total"],
    )

    # written by hand from the table above, not copied from a run
    expected_df = spark.createDataFrame(
        [
            ("Zwolle", "Alpha", 500.0, 1),
            ("Zwolle", "Bravo", 300.0, 2),
            ("Zwolle", "Charlie", 300.0, 2),
            ("Assen", "Echo", 90.0, 1),
        ],
        "city string, movie_title string, total_revenue double, rn int",
    )

    result_df = top_movies_per_city(input_df)
    assertDataFrameEqual(result_df, expected_df)
