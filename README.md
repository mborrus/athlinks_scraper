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
athlinks-scraper "https://www.athlinks.com/event/..."
```

### Dashboard
```bash
cd dashboard
pip install -r requirements.txt
streamlit run app.py
```
