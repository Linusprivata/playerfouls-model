"""Download FBref player & team match tables, save to Parquet."""
from loguru import logger
from tqdm import tqdm

from pathlib import Path
import pandas as pd
import polars as pl
from soccerdata import FBref
import os, certifi

# ── force Requests / OpenSSL to trust Mozilla’s CA bundle ─────────
os.environ["SSL_CERT_FILE"] = certifi.where()
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()


import pyarrow.parquet as pq

def test_raw_files_exist():
    for y in (2021, 2022, 2023, 2024):
        path = Path(f"data/raw/fbref/player_match_{y}_{y+1}.parquet")
        assert path.exists()
        assert pq.ParquetFile(path).metadata.num_rows > 0


RAW_DIR = Path("data/raw/fbref")
SEASONS = range(2021, 2024)

def fetch_player(season: int):
    fb = FBref(leagues="ENG-Premier League", seasons=[season])   # ← verify removed
    return fb.read_player_match_stats()

def fetch_team(season: int):
    fb = FBref(leagues="ENG-Premier League", seasons=[season])   # ← verify removed
    return fb.read_team_match_stats()


def clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [
        "_".join(col).strip().lower().replace(".", "_") if isinstance(col, tuple) else col.lower().replace(".", "_")
        for col in df.columns
    ]
    return df

def coerce_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """Convert object columns that are mostly digits into nullable Int64."""
    for col in df.columns[df.dtypes == "object"]:
        series = df[col].astype(str).str.strip()
        # heuristic: if ≥90 % of the first 100 non-null cells are digits or '-', treat as numeric
        sample = series.dropna().head(100)
        if (sample.str.replace("-", "").str.isnumeric().mean() > 0.9):
            df[col] = pd.to_numeric(series.replace("-", pd.NA), errors="coerce").astype("Int64")
    return df


from concurrent.futures import ThreadPoolExecutor #faster parallel processing
from tqdm import tqdm

def fetch_and_save(season):
    logger.info(f"Fetching season {season}-{season+1}")
    player = clean_cols(fetch_player(season))
    team   = clean_cols(fetch_team(season))

    player.to_parquet(
        RAW_DIR / f"player_match_{season}_{season+1}.parquet",
        engine="pyarrow", index=False
    )
    team.to_parquet(
        RAW_DIR / f"team_match_{season}_{season+1}.parquet",
        engine="pyarrow", index=False
    )

if __name__ == "__main__":


    # RAW_DIR.mkdir(parents=True, exist_ok=True) #faster parallel processing

    # with ThreadPoolExecutor(max_workers=4) as executor:
    #     list(tqdm(executor.map(fetch_and_save, SEASONS), total=len(SEASONS), desc="Downloading seasons"))
    
    
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    for s in tqdm(SEASONS, desc="Downloading seasons"):
        logger.info(f"Fetching season {s}-{s+1}")

        player = coerce_numeric(clean_cols(fetch_player(s)))
        team   = coerce_numeric(clean_cols(fetch_team(s)))

        pl.from_pandas(player).write_parquet(RAW_DIR / f"player_match_stats_{s}_{s+1}.parquet")
        pl.from_pandas(team).write_parquet  (RAW_DIR / f"team_match_stats_{s}_{s+1}.parquet")

        logger.success(f"Saved player & team match stats for {s}-{s+1}")
