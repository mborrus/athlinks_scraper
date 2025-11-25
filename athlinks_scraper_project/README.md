# Athlinks Scraper

A Python package to scrape race results from Athlinks.com and export them to CSV.

## Features

-   Fetches all results for a given event.
-   Handles pagination automatically.
-   Calculates **Pace** (min/mi) for each runner.
-   Exports data to a clean CSV file.

## Installation

1.  Navigate to the project directory:
    ```bash
    cd athlinks_scraper_project
    ```

2.  Install the package in editable mode:
    ```bash
    pip install -e .
    ```

## Usage

Once installed, you can use the `athlinks-scraper` command from anywhere.

### Basic Usage

```bash
athlinks-scraper "https://www.athlinks.com/event/15776/results/Event/1096764/Results"
```

This will generate a `results.csv` file in your current directory.

### Specify Output File

You can specify a custom output filename using the `--output` or `-o` flag:

```bash
athlinks-scraper "https://www.athlinks.com/event/15776/results/Event/1096764/Results" --output my_race_results.csv
```

### Specify Output Directory

You can specify an output directory using the `--output-dir` or `-d` flag. The filename will be auto-generated from the event name.

```bash
athlinks-scraper "https://www.athlinks.com/event/15776/results/Event/1096764/Results" --output-dir ./data
```

### Scrape All Years

If you provide a Master Event URL (e.g., `https://www.athlinks.com/event/15776`), you can use the `--all-years` flag to scrape results for all available years.

```bash
athlinks-scraper "https://www.athlinks.com/event/15776" --all-years
```

### Running without Installation

If you prefer not to install the package, you can run it directly using Python:

```bash
# From the parent directory
PYTHONPATH=athlinks_scraper_project python -m athlinks_scraper.cli "URL_HERE"
```

## Output Format

The generated CSV contains the following columns:
-   Name
-   Gender
-   Age
-   Bib
-   City, State, Country
-   Time (Chip time)
-   Pace (min/mi)
-   Overall Rank
-   Gender Rank
-   Division Rank
-   Status

## Visualize Your Data

Check out the [Race Analytics Dashboard](../dashboard/README.md) to visualize your results, find pace partners, and more!
