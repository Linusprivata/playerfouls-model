
from pathlib import Path
import pyarrow.parquet as pq

def test_raw_files_exist():
    for y in (2021, 2022, 2023, 2024):
        path = Path(f"data/raw/fbref/player_match_{y}_{y+1}.parquet")
        assert path.exists()
        assert pq.ParquetFile(path).metadata.num_rows > 0



def test_stub():
 assert True
