from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

def top_n_per_city(df, n=3):
    w = Window.partitionBy("city").orderBy(col("revenue").desc(), col("restaurant_name").asc())
    return (df
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") <= n)
        .drop("rn")
    )
