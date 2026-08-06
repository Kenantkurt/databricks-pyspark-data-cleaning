from pyspark.sql.functions import col, when, lit, trim, replace


def clean_price(df):
    # placeholders -> null
    df = df.withColumns({
        "total_price": when(col("total_price").isin("N/A", "ERROR", "UNKNOWN"), None)
                       .otherwise(col("total_price"))
    })

    # clean first: strip currency junk, normalize decimal comma
    df = df.withColumns({
        "total_price": trim(replace(replace(replace(col("total_price"),
                       lit("EUR"), lit("")), lit("€"), lit("")), lit(","), lit(".")))
    })

    # cast last
    df = df.withColumns({
        "total_price": col("total_price").cast("double")
    })

    return df
