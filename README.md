# Athlinks Scraper & Analytics

This repository contains tools to scrape race results from Athlinks.com and a dashboard to analyze and visualize them.

## Components

### 1. [Athlinks Scraper](athlinks_scraper_project/README.md)
A Python package to fetch race results, handle pagination, and export data to CSV.

- **Features**: Scrapes all results, calculates pace, exports to CSV.
- **Location**: `athlinks_scraper_project/`

### 2. [Race Analytics Dashboard](dashboard/README.md)
A Streamlit-based dashboard to explore your race data.

- **Features**: Performance trends, pace distribution, runner lookup, pace partners.
- **Location**: `dashboard/`

## Quick Start

### Scraper
```bash
cd athlinks_scraper_project
pip install -e .
athlinks-scraper "https://www.athlinks.com/event/15776" --all-years -d out/
athlinks-scraper "https://results.nyrr.org/event/24FROSTY/finishers" --all-years -d out/
athlinks-scraper "https://runsignup.com/Race/Results/100692" --all-years -d out/
```

Supported sources: Athlinks (`athlinks.com`), New York Road Runners (`results.nyrr.org`),
RunSignup (`runsignup.com` — use the *Results* page URL, which contains the numeric race id).
None require an API key.

### Dashboard
From the repository root:

```bash
pip install -r requirements-dev.txt
streamlit run dashboard/app.py
```

See `dashboard/README.md` for the MotherDuck deployment.

## Development

Install everything (both components plus pytest) from the repository root:

```bash
pip install -r requirements-dev.txt
```

Run the test suite from the repository root (not from a subdirectory):

```bash
python -m pytest tests -v
```

The tests never contact Athlinks; HTTP is faked in `tests/fakes.py`. The dashboard
query layer is tested against a small in-memory DuckDB table built in `tests/conftest.py`.

`test_url.py` and `test_metadata.py` at the repo root are ad-hoc debugging scripts that
hit the live API; they are not part of the test suite.

Every push and pull request runs the test suite on Python 3.9 and 3.11 via
GitHub Actions (`.github/workflows/tests.yml`).
