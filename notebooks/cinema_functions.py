from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, col, sum


def top_movies_per_city(df, n=2):
    """Top-n movies per city by revenue.

    dense_rank is deliberate: on a revenue tie both movies keep the same rank,
    so a "top 2" can return three rows for that city. If the contract required
    exactly n rows, row_number() with an explicit tie-breaker would be the choice.
    """
    df = df.groupBy("city", "movie_title").agg(
        sum("price_total").alias("total_revenue")
    )
    w = Window.partitionBy("city").orderBy(col("total_revenue").desc())
    df = df.withColumn("rn", dense_rank().over(w))
    df = df.filter(col("rn") <= n)
    return df
