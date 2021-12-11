#%%
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import polars as pl

#%%
df: pl.DataFrame = pl.from_pandas(sns.load_dataset("tips")).lazy()
df

#%%
clean = df.with_columns(
    [
        (pl.col("tip") / pl.col("total_bill")).alias("tip_pct"),
        (pl.col("size") == 1).alias("single"),
        (pl.col("day").cast(pl.Utf8).is_in(pl.Series(values=["Sun", "Sat"]))).alias(
            "weekend"
        ),
    ]
)

clean.collect()

#%%
clean.groupby("sex").agg([pl.col("tip_pct").mean()]).collect()


#%%
clean.map(
    lambda df: df.groupby("sex")  # .with_column(pl.col("time").cast(pl.Utf8))
    .pivot("time", "tip_pct")
    .mean()
).collect()

#%%
clean.collect().groupby("sex").pivot("time", "tip_pct").mean()

#%%
dir(clean.collect().select("time"))
