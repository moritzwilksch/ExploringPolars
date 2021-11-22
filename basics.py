#%%
import polars as pl
import re

re_cashtags = re.compile(r"\$[A-Z]+")

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
RERUN_CLEAN_FROM_CSV = False

if RERUN_CLEAN_FROM_CSV:
    df: pl.DataFrame = pl.scan_csv("data/TSLA_tweets.csv")
    clean: pl.DataFrame = (
        df.pipe(fix_datetime)
        .pipe(clean_tweet)
        .pipe(clean_vector)
        .with_column(pl.col("cashtags").apply(list_from_strlist).alias("n_cashtags"))
        .collect()
    )
    clean.to_parquet("data/clean_tweets.parquet")
    clean: pl.LazyFrame = clean.lazy()
else:
    clean: pl.LazyFrame = pl.scan_parquet("data/clean_tweets.parquet")


#%%
res = clean.groupby("username").agg(pl.col("id").n_unique()).sort("id_n_unique")
res.collect()

#%%
nct_peruser = clean.groupby("username").agg(pl.col("n_cashtags").mean()).sort("n_cashtags_mean")

nct_peruser.select(['n_cashtags_mean']).quantile(0.95).collect()