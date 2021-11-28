# %%
import pandas as pd
from sklearn.compose import ColumnTransformer, make_column_transformer
from sklearn.preprocessing import FunctionTransformer
from sklearn.pipeline import Pipeline

#%%
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
def start_pipeline(data):
    print("Start pipeline")
    return data.copy()

def fix_dtypes(data: pd.DataFrame, dtype) -> pd.DataFrame:
    print("Fix dtypes")
    data = data.astype(dtype)
    return data


# def clean_object_cols(data: pd.DataFrame) -> pd.DataFrame:
#     print(f"[PROCESSING] Cleaning obj-cols...")
#     data["hashtags"] = data.hashtags.apply(
#         lambda x: ", ".join(x.split("', '")).strip("[']")
#     ).astype("string")
#     data.loc[data.hashtags.str.len() == 0, "hashtags"] = pd.NA

#     data["cashtags"] = data.cashtags.apply(
#         lambda x: ", ".join(x.split("', '")).strip("[']")
#     ).astype("string")
#     data.loc[data.cashtags.str.len() == 0, "cashtags"] = pd.NA

#     return data

import numba



def clean_obj_col(data, col):
    print("Cleaning obj cols")
    data[col].apply(strclean).astype("string")
    data.loc[data[col].apply(len) == 0, col] = pd.NA
    return data



def drop_dupes(data: pd.DataFrame) -> pd.DataFrame:
    """Drop dupes."""
    print("Drop dupes")
    len_before = len(data)
    data = data.drop_duplicates(subset="id")
    len_after = len(data)

    print(f"[PROCESSING] Dropped {len_before - len_after} duplicates...")

    return data

#%%

pl = Pipeline([
    ('start', FunctionTransformer(start_pipeline)),
    ('dropdupes', FunctionTransformer(drop_dupes)),
    ('fixtypes', ColumnTransformer([
        ("strcols", FunctionTransformer(fix_dtypes, kw_args=dict(dtype="string")), ["username", "name", "tweet", "link"]),
        ("cat", FunctionTransformer(fix_dtypes, kw_args=dict(dtype="category")), ["language"]),
    ], remainder='passthrough', n_jobs=-1)),
    ("setcols1", FunctionTransformer(lambda x: pd.DataFrame(x, columns=df.columns))),
    ("fixobjtypes", ColumnTransformer([
        ("clean_objcols1", FunctionTransformer(clean_obj_col, kw_args=dict(col="hashtags")), ["hashtags"]),
        ("clean_objcols2", FunctionTransformer(clean_obj_col, kw_args=dict(col="cashtags")), ["cashtags"]),
    ], remainder='passthrough', n_jobs=-1)),
    ("setcols2", FunctionTransformer(lambda x: pd.DataFrame(x, columns=df.columns))),
])

#%%
# pl.fit_transform(df)

#%%
clean_obj_col(df.copy(), "cashtags")

#%%
%%time
@numba.njit()
def strclean(x: str) -> str:
    return ", ".join(x.split("', '")).strip("[']")
strclean("dafs', 'sadf")

#%%
df['cashtags'].apply(strclean)

#%%
from joblib import Parallel, delayed
#%%
%%time
Parallel(n_jobs=-1)(delayed(strclean)(x) for x in df['cashtags'])

#%%
%%time
df.cashtags.apply(strclean)