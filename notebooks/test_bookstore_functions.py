import pytest
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual
from bookstore_functions import customers_without_orders


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()


def test_customers_without_orders(spark):
    customers_df = spark.createDataFrame(
        [
            ("C1", "Anna"),
            ("C2", "Bram"),
            ("C3", "Chloe"),
            ("C4", "Daan")
        ],
        ["customer_id", "customer_name"]
    )

    orders_df = spark.createDataFrame(
        [
            ("01", "C1"),
            ("02", "C1"),
            ("03", "C3")
        ], ["order_id", "customer_id"]
    )

    # expected written BY HAND: C1 and C3 have orders, so only C2 and C4 remain
    expected_df = spark.createDataFrame(
        [
            ("C2", "Bram"),
            ("C4", "Daan")
        ], ["customer_id", "customer_name"]
    )

    actual_df = customers_without_orders(customers_df, orders_df)
    assertDataFrameEqual(actual_df, expected_df)


def test_customers_with_orders(spark):
    # edge case: every customer has an order -> the function must return 0 rows
    customers_df = spark.createDataFrame(
        [
            ("C1", "Anna"),
            ("C3", "Chloe")
        ], ["customer_id", "customer_name"]
    )

    orders_df = spark.createDataFrame(
        [
            ("01", "C1"),
            ("02", "C1"),
            ("03", "C3")
        ], ["order_id", "customer_id"]
    )

    actual_df = customers_without_orders(customers_df, orders_df)
    assert actual_df.count() == 0
