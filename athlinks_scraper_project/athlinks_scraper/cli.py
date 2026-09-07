import argparse
import os
import re
import sys

from .providers import detect_provider


def sanitize_filename(name):
    """Removes filesystem-unsafe characters and replaces spaces with underscores."""
    name = re.sub(r'[<>:"/\\|?*]', '', name)
    return name.replace(' ', '_')


def output_path_for(ref, df, output_dir=None, output_file=None):
    if output_file:
        return output_file
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        year = ref.date_str[:4] if ref.date_str and ref.date_str != "Unknown" else "unknown"
        safe = sanitize_filename(f"{ref.source}_{ref.race_group}_{year}_{ref.event_id}")
        return os.path.join(output_dir, f"{safe}.csv")
    return "results.csv"


def process_ref(provider, ref, output_dir=None, output_file=None):
    print(f"Scraping {ref.name} ({ref.date_str}) from {ref.source}...")
    df = provider.fetch_event(ref)
    if df.empty:
        print(f"No results found for {ref.name}")
        return
    path = output_path_for(ref, df, output_dir, output_file)
    df.to_csv(path, index=False)
    print(f"Successfully saved {len(df)} rows to {path}")


def main():
    parser = argparse.ArgumentParser(description="Scrape race results (Athlinks, NYRR, RunSignup) to CSV.")
    parser.add_argument("url", help="A results URL from athlinks.com, results.nyrr.org or runsignup.com.")
    parser.add_argument("--output", "-o", help="Output CSV filename (single event only).")
    parser.add_argument("--output-dir", "-d", help="Output directory; filenames are generated per event.")
    parser.add_argument("--all-years", action="store_true", help="Scrape every event found, not just the newest.")
    args = parser.parse_args()

    try:
        provider = detect_provider(args.url)
        refs = provider.list_events(args.url)
        if not refs:
            print("No events found for that URL.")
            return

        targets = refs if args.all_years else refs[:1]
        print(f"Found {len(refs)} events on {provider.name}; scraping {len(targets)}.")
        failures = 0
        for ref in targets:
            try:
                process_ref(provider, ref, args.output_dir, args.output)
            except Exception as e:  # keep going; report at the end
                failures += 1
                print(f"Failed to scrape {ref.name}: {e}")
        if failures:
            print(f"Done with {failures} failure(s).")
            sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
