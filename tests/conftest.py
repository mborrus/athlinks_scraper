"""Shared pytest configuration.

Both components live in subdirectories and the dashboard is not an installable
package, so we put both source directories on sys.path here. This runs before
any test module is imported.
"""
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRAPER_DIR = os.path.join(REPO_ROOT, "athlinks_scraper_project")
DASHBOARD_DIR = os.path.join(REPO_ROOT, "dashboard")

for path in (SCRAPER_DIR, DASHBOARD_DIR):
    if path not in sys.path:
        sys.path.insert(0, path)
