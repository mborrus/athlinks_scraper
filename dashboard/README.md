# Athlinks Race Analytics Dashboard

A powerful Streamlit-based dashboard for analyzing race results, finding pace partners, and visualizing performance trends.

## Features

- **Race Overview**: Get quick stats on total runners, average pace, and fastest/slowest times.
- **Performance Trends**: Visualize how pace metrics (min, max, median) evolve over the years.
- **Pace Distribution**: View a histogram of pace distribution across all runners.
- **Runner Lookup**: Search for specific runners to see their race history and performance.
- **Pace Partners**: Input your target pace to find other runners who finish near your time—perfect for finding training buddies or rivals!
- **Hall of Fame**: Discover the "Frequent Flyers" who have raced the most times.

## Run locally

From the repository root:

```bash
pip install -r requirements-dev.txt
streamlit run dashboard/app.py
```

With no MotherDuck token configured, results are stored in
`dashboard/data/results.duckdb` (git-ignored). Add a race from the sidebar by
pasting an Athlinks, NYRR, or RunSignup results URL, or upload CSVs and click
**Import CSVs**. Scraped data persists across restarts.

The local file store is single-process: stop the app before running
`migrate_files.py` against it, or DuckDB will refuse to open the file. MotherDuck
has no such limit.

Have old `scraped_*.parquet` files? Import them once:

```bash
python dashboard/migrate_files.py            # reads dashboard/data/*.parquet|csv
```

`restore_data.py` (repo root) writes legacy parquet files into `dashboard/data/`;
run it before `migrate_files.py` if you use it.

## Deploy (Streamlit Community Cloud + MotherDuck)

The hosted container has no durable disk, so results live in a free
MotherDuck database (10 GB on the Lite plan). The app pulls one race at a time
into local DuckDB, so MotherDuck compute stays near zero.

1. Create a MotherDuck account and an access token (Settings → Access Tokens).
2. In Streamlit Community Cloud, deploy this repo with main file
   `dashboard/app.py`.
3. In the app's **Settings → Secrets**, paste:

   ```toml
   [motherduck]
   token = "your-token"
   ```

4. To seed the cloud database with files from your laptop, run once:

   ```bash
   MOTHERDUCK_TOKEN=your-token python dashboard/migrate_files.py
   ```

5. `migrate_files.py` reads `.streamlit/secrets.toml` only on Python 3.11+
   (or when the `tomli` package is installed). On older Pythons, pass the
   token via the `MOTHERDUCK_TOKEN` environment variable as shown above.

Anyone with the app URL can scrape or rename races; there is no login.

## Tech Stack

- **Streamlit**: For the interactive web interface.
- **DuckDB**: For fast, in-memory SQL querying of data.
- **Pandas**: For data manipulation.
- **Plotly**: For interactive charts and graphs.
