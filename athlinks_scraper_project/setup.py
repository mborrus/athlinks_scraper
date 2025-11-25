from setuptools import setup, find_packages

setup(
    name="athlinks-scraper",
    version="0.1.0",
    description="A Python package to scrape race results from Athlinks.",
    author="Antigravity",
    packages=find_packages(),
    install_requires=[
        "requests",
        "pandas",
    ],
    entry_points={
        "console_scripts": [
            "athlinks-scraper=athlinks_scraper.cli:main",
        ],
    },
)
