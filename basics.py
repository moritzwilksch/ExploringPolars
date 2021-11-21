#%%
import polars as pl
import re

re_cashtags = re.compile(r"\$[A-Z]+")

df: pl.DataFrame = pl.scan_csv("data/TSLA_tweets.csv")
# df: pl.DataFrame = pl.read_csv("data/TSLA_tweets.csv")

#%%
df.head(1)

#%%
def fix_datetime(dataf: pl.DataFrame) -> pl.DataFrame:
    return dataf.with_column(
        pl.col("created_at").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S %Z")
    ).drop(["date", "time", "timezone"])


def clean_tweet(dataf: pl.DataFrame) -> pl.DataFrame:
    return dataf.with_column(pl.col("tweet").str.replace("@[\w_\d]+", "@user"))


def clean_vector(dataf: pl.DataFrame) -> pl.DataFrame:
    url_regex = r"((https?):((//)|(\\\\))+[\w\d:#@%/;$()~_?\+-=\\\.&]*)"
    return dataf.with_column(pl.col("tweet").str.replace(url_regex, ""))


def get_cashtags(s: str) -> list:
    return re_cashtags.findall(s)


def extract_cashtags(dataf: pl.DataFrame) -> pl.DataFrame:
    return dataf.with_column(pl.col("tweet").apply(get_cashtags))


def list_from_strlist(s: str) -> int:
    return len(s.strip("[]").replace("'", "").split(","))


#%%
clean: pl.DataFrame = (
    df.pipe(fix_datetime)
    .pipe(clean_tweet)
    .pipe(clean_vector)
    .with_column(pl.col("cashtags").apply(list_from_strlist).alias("n_cashtags"))
    .collect()
).lazy()

#%%
clean.select(("cashtags", "n_cashtags")).collect()

#%%
clean.with_column(pl.col("username").cast(pl.Categorical).alias("username_cat")).groupby(
    "username_cat"
).agg(pl.col("id").n_unique()).sort("id_n_unique").collect()
