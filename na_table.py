#%%
import polars as pl

#%%
df = pl.read_parquet(
    "https://github.com/moritzwilksch/BerlinRealEstatePrices/raw/main/data/berlin_clean.parquet"
)

#%%
maskdf = df.with_columns(
    [pl.col(c).is_null().cast(pl.UInt16).alias(f"{c}_null") for c in df.columns]
).select("^.*_null$")
maskdf.melt(maskdf.columns, maskdf.columns).groupby("variable").pivot("variable", "value").sum()
