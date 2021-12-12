#%%
import polars as pl
from polars.datatypes import Datetime

df = pl.scan_csv("orders.csv", sep=";")

#%%
df.head(10).collect()

#%%
def fix_datetime(data: pl.LazyFrame) -> pl.LazyFrame:
    return data.with_column(
        (pl.col("date") + " " + pl.col("time"))
        .str.strptime(pl.Datetime)
        .alias("datetime")
    ).drop(["date", "time"])


def fix_dtypes(data: pl.LazyFrame) -> pl.LazyFrame:
    return data.with_columns(
        [
            pl.col("item_name").cast(pl.Categorical),
            pl.col("category_name").cast(pl.Categorical),
            pl.col("item_price") / 100,
        ]
    )


def subset(data: pl.LazyFrame) -> pl.LazyFrame:
    return data.filter(pl.col("cancelled") == 0).select(
        [
            "id",
            "item_name",
            "item_price",
            "category_name",
            "datetime",
        ]
    )


clean = df.pipe(fix_datetime).pipe(fix_dtypes).pipe(subset)

#%%
clean.groupby("item_name").agg(
    [pl.col("item_price").sum(), pl.col("item_price").count()]
).sort("item_price_sum", reverse=True).collect()

#%%
clean.select([pl.col(c).is_null().alias(f"{c}_na") for c in clean.columns]).sum().collect()