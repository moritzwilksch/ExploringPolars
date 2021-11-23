# %%
import pandas as pd

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
df = pd.read_csv(root_path + f"{ticker}_tweets.csv", usecols=keep_cols)


#%%
def fix_dtypes(data: pd.DataFrame) -> pd.DataFrame:
    print(f"[PROCESSING] Fixing dtypes...")
    data = data.assign(created_at=pd.to_datetime(data.date + " " + data.time))
    data = data.drop(["date", "time"], axis=1)

    string_cols = ["username", "name", "tweet", "link"]
    data[string_cols] = data[string_cols].astype("string")

    data["language"] = data["language"].astype("category")
    return data


def clean_object_cols(data: pd.DataFrame) -> pd.DataFrame:
    print(f"[PROCESSING] Cleaning obj-cols...")
    data["hashtags"] = data.hashtags.apply(
        lambda x: ", ".join(x.split("', '")).strip("[']")
    ).astype("string")
    data.loc[data.hashtags.str.len() == 0, "hashtags"] = pd.NA

    data["cashtags"] = data.cashtags.apply(
        lambda x: ", ".join(x.split("', '")).strip("[']")
    ).astype("string")
    data.loc[data.cashtags.str.len() == 0, "cashtags"] = pd.NA

    return data


def drop_dupes(data: pd.DataFrame) -> pd.DataFrame:
    """Drop dupes."""
    len_before = len(data)
    data = data.drop_duplicates(subset="id")
    len_after = len(data)

    print(f"[PROCESSING] Dropped {len_before - len_after} duplicates...")

    return data

#%%
%%time
clean: pd.DataFrame = (
    df.pipe(fix_dtypes)
    .pipe(drop_dupes)
    .pipe(clean_object_cols)
    # .query("language == 'en'") # filtered later
)

#%%
from joblib import Parallel, delayed

pardf =