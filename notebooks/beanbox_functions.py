from pyspark.sql.functions import expr,col


def revenue_per_category(df):
    result = df.groupBy("category").agg(
        expr("sum(qty * unit_price) as total_revenue")
    ).orderBy(col("total_revenue").desc())
    return result

