from pyspark.sql.functions import expr


def customers_without_orders(customers_df, orders_df):
    df = (
        customers_df.alias("c")
        .join(orders_df.alias("o"), on=expr("c.customer_id = o.customer_id"), how="left_anti")
    )
    return df.select("customer_id", "customer_name")
