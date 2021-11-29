# %%
import polars as pl
import numpy as np

root_path = "./"
ticker = "TSLA"

keep_cols = [
    "id",
    "created_at",
    "date",
    "time",
    "user_id",
    "username",
    "name",
    "tweet",
    "language",
    "replies_count",
    "retweets_count",
    "likes_count",
    "hashtags",
    "cashtags",
    "link",
]

#%%
df = pl.scan_csv(root_path + f"{ticker}_tweets.csv").select(pl.col(keep_cols))


#%%
def fix_dtypes(data):
    print(f"[PROCESSING] Fixing dtypes...")
    # data = data.assign(created_at=pl.to_datetime(data.date + " " + data.time))
    string_cols = ["username", "name", "tweet", "link"]
    data = (
        data.with_column(pl.col("created_at").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"))
        .drop(["date", "time"])
        .with_column(pl.col(string_cols).cast(pl.Utf8))
        .with_column(pl.col("language").cast(pl.Categorical))
    )

    return data


def clean_object_cols(data):
    print(f"[PROCESSING] Cleaning obj-cols...")

    for objcol in ["hashtags", "cashtags"]:
        data = data.with_column(
            pl.col(objcol).apply(lambda x: x.strip("[']").split("', '"))
        )
        data.with_column(
            pl.when(pl.col(objcol).eq(pl.lit(0))).then(np.nan).otherwise(pl.col(objcol))
        )

    return data


def drop_dupes(data):
    """Drop dupes."""
    # len_before = len(data)
    # data = data.drop_duplicates(subset="id")
    data = data.drop_duplicates(subset="id")
    # len_after = len(data)

    # print(f"[PROCESSING] Dropped {len_before - len_after} duplicates...")

    return data


#%%
clean: pl.DataFrame = (
    df.pipe(fix_dtypes)
    .pipe(drop_dupes)
    .pipe(clean_object_cols)
    # .query("language == 'en'") # filtered later
).collect()

print("Done!")


#%%
clean.with_column(pl.col("hashtags").apply(lambda a: pl.Series(a)))


#%%
x = pl.DataFrame({'a': [1, 2], 'b': [['abc', 'def'], ['ghi', 'jkl']]})
x.with_column(pl.col('b').cast(pl.Object))

#%%
clean.select("hashtags").to_dict()['hashtags'].cast(pl.List).apply(len)