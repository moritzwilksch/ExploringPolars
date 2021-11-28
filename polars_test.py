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
%%time
df = pl.scan_csv(root_path + f"{ticker}_tweets.csv").select(pl.col(keep_cols))


#%%
def fix_dtypes(data):
    print(f"[PROCESSING] Fixing dtypes...")
    # data = data.assign(created_at=pl.to_datetime(data.date + " " + data.time))
    data = data.with_column(pl.col("created_at").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"))
    data = data.drop(["date", "time"])

    string_cols = ["username", "name", "tweet", "link"]
    data = data.with_column(pl.col(string_cols).cast(pl.Utf8))

    data = data.with_column(pl.col("language").cast(pl.Categorical))
    return data


def clean_object_cols(data):
    print(f"[PROCESSING] Cleaning obj-cols...")
    # data["hashtags"] = data.hashtags.apply(
    #     lambda x: ", ".join(x.split("', '")).strip("[']")
    # ).astype("string")

    data = data.with_column(pl.col("hashtags").apply(lambda x: ", ".join(x.split("', '")).strip("[']")).cast(pl.Utf8))
    # data = data.filter(pl.col("hashtags").apply(lambda x: len(x)) == 0).select("hashtags").set(np.nan)
    data.with_column(pl.when(pl.col("hashtags").eq(0), np.nan).otherwise(pl.col("hashtags")))

    # data.loc[data.hashtags.str.len() == 0, "hashtags"] = pd.NA

    # data["cashtags"] = data.cashtags.apply(
    #     lambda x: ", ".join(x.split("', '")).strip("[']")
    # ).astype("string")
    # data.loc[data.cashtags.str.len() == 0, "cashtags"] = pd.NA

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
%%time
clean = (
    df.pipe(fix_dtypes)
    .pipe(drop_dupes)
    .pipe(clean_object_cols)
    # .query("language == 'en'") # filtered later
).collect()
