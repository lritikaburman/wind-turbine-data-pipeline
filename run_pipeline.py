from pathlib import Path

from src.pipeline import run_pipeline


if __name__ == "__main__":
    root = Path(__file__).resolve().parent
    run_pipeline(str(root / "data" / "data_group_*.csv"))
