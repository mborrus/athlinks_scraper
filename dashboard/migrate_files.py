"""
One-shot import of legacy scraped files into the results store.

    python dashboard/migrate_files.py                 # imports dashboard/data/*.parquet|csv
    python dashboard/migrate_files.py --data-dir PATH

The store is chosen the same way the app chooses it: MOTHERDUCK_TOKEN in the
environment (or [motherduck] token in .streamlit/secrets.toml) means
MotherDuck; otherwise the local file dashboard/data/results.duckdb.
Exit status is 1 if any file failed.
"""
import argparse
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import storage  # noqa: E402


def _secrets_from_toml():
    path = os.path.join(HERE, "..", ".streamlit", "secrets.toml")
    if not os.path.exists(path):
        return {}
    try:
        import tomllib  # Python 3.11+
    except ImportError:
        try:
            import tomli as tomllib  # type: ignore
        except ImportError:
            return {}
    with open(path, "rb") as f:
        return tomllib.load(f)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data-dir", default=storage.DEFAULT_DATA_DIR)
    args = parser.parse_args(argv)

    dsn = storage.resolve_dsn(secrets=_secrets_from_toml(), env=os.environ, data_dir=args.data_dir)
    print("store:", "MotherDuck" if dsn.startswith("md:") else dsn)
    con = storage.open_store(dsn)
    report = storage.import_files(con, args.data_dir)
    names = storage.import_metadata_json(con, os.path.join(args.data_dir, "event_metadata.json"))
    if names:
        print(f"event_metadata.json: {names} display names")
    if not report:
        print("no .parquet/.csv files found in", args.data_dir)
        return 0
    failed = 0
    for filename, rows in report:
        print(f"{filename}: {'FAILED' if rows < 0 else f'{rows} rows'}")
        failed += rows < 0
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
