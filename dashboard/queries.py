import duckdb
import pandas as pd

def init_db(uploaded_files):
    """
    Initializes an in-memory DuckDB connection and loads CSV files.
    Returns the connection object.
    """
    con = duckdb.connect(database=':memory:')
    
    # Create a list to hold all dataframes
    dfs = []
    
    for uploaded_file in uploaded_files:
        try:
            df = pd.read_csv(uploaded_file)
            # Ensure column names are consistent/clean
            df.columns = [c.strip() for c in df.columns]
            dfs.append(df)
        except Exception as e:
            print(f"Error loading {uploaded_file.name}: {e}")
            
    if dfs:
        # Concatenate all dataframes
        full_df = pd.concat(dfs, ignore_index=True)
        
        # Register as a DuckDB table
        con.register('results', full_df)
        
        # Create a view with some calculated fields
        # Handle Pace parsing more robustly. 
        # We try to parse MM:SS. If it fails (e.g. HH:MM:SS), we might need another approach, 
        # but for 5Ks MM:SS is standard.
        # Also normalize Race Type: 5k, run-5k, RUN-5K -> 5K
        # FIX: Use SPLIT_PART instead of strptime to handle minutes > 59 (e.g. "63:05")
        con.execute("""
            CREATE OR REPLACE VIEW results_enriched AS
            SELECT *,
                 CASE 
                    WHEN "Pace" LIKE '%:%:%' THEN 
                        TRY_CAST(SPLIT_PART("Pace", ':', 1) AS INTEGER) * 3600 + 
                        TRY_CAST(SPLIT_PART("Pace", ':', 2) AS INTEGER) * 60 + 
                        TRY_CAST(SPLIT_PART("Pace", ':', 3) AS INTEGER)
                    ELSE 
                        TRY_CAST(SPLIT_PART("Pace", ':', 1) AS INTEGER) * 60 + 
                        TRY_CAST(SPLIT_PART("Pace", ':', 2) AS INTEGER)
                 END as pace_seconds,
                 
                 YEAR(CAST("Event Date" AS DATE)) as event_year,
                 CASE 
                    WHEN REGEXP_MATCHES("Race Type", '(?i)^(run[- ]?)?5k$') THEN '5K'
                    ELSE "Race Type"
                 END as "Race Type Normalized"
            FROM results
            WHERE "Pace" IS NOT NULL AND "Pace" != ''
        """)
        
    return con

def get_overview_stats(con):
    """
    Returns basic stats: Total Runners, Avg Time, Fastest Time.
    """
    try:
        query = """
            SELECT 
                COUNT(*) as total_runners,
                AVG(pace_seconds) as avg_pace_seconds,
                MIN("Time") as fastest_time,
                MAX("Time") as slowest_time
            FROM results_enriched
        """
        return con.execute(query).df()
    except Exception:
        return pd.DataFrame()

def get_pace_partners(con, target_pace_str, tolerance_seconds=10):
    """
    Finds runners who finish near the target pace.
    target_pace_str: "MM:SS"
    tolerance_seconds: +/- seconds to include
    """
    try:
        # Robust parsing of target pace
        parts = target_pace_str.split(':')
        if len(parts) == 2:
            m, s = map(int, parts)
            target_seconds = m * 60 + s
        elif len(parts) == 1:
            # Assume minutes if just one number
            target_seconds = int(parts[0]) * 60
        else:
            return pd.DataFrame()
        
        min_sec = target_seconds - tolerance_seconds
        max_sec = target_seconds + tolerance_seconds
        
        # Get max year to filter for last 2 years
        max_year_res = con.execute("SELECT MAX(event_year) FROM results_enriched").fetchone()
        max_year = max_year_res[0] if max_year_res else None
        
        year_clause = ""
        if max_year:
            cutoff_year = max_year - 1
            year_clause = f"AND event_year >= {cutoff_year}"
        
        query = f"""
            SELECT "Name", "Pace", "Time", "Event Date", "Race Type"
            FROM results_enriched
            WHERE pace_seconds BETWEEN {min_sec} AND {max_sec}
            {year_clause}
            ORDER BY ABS(pace_seconds - {target_seconds}) ASC
            LIMIT 20
        """
        return con.execute(query).df()
    except Exception as e:
        print(f"Error finding pace partners: {e}")
        return pd.DataFrame()

def get_fun_stats(con):
    """
    Returns some fun stats like most frequent runners.
    """
    try:
        # Hall of Fame (Most Races)
        # Only useful if multiple files loaded
        hall_of_fame = con.execute("""
            SELECT "Name", COUNT(*) as race_count, MIN("Pace") as best_pace
            FROM results_enriched
            GROUP BY "Name"
            HAVING count(*) > 1
            ORDER BY race_count DESC, best_pace ASC
            LIMIT 10
        """).df()
        
        return hall_of_fame
    except Exception:
        return pd.DataFrame()

def get_distribution(con):
    """
    Returns data for pace distribution histogram.
    """
    try:
        return con.execute("""
            SELECT pace_seconds / 60.0 as pace_minutes
            FROM results_enriched
            WHERE pace_seconds IS NOT NULL
        """).df()
    except Exception:
        return pd.DataFrame()

def get_trends(con):
    """
    Aggregates stats by Year.
    """
    try:
        return con.execute("""
            SELECT 
                event_year,
                COUNT(*) as runner_count,
                MIN(pace_seconds)/60.0 as min_pace_min,
                MAX(pace_seconds)/60.0 as max_pace_min,
                MEDIAN(pace_seconds)/60.0 as median_pace_min
            FROM results_enriched
            WHERE event_year IS NOT NULL
            GROUP BY event_year
            ORDER BY event_year
        """).df()
    except Exception as e:
        print(f"Error getting trends: {e}")
        return pd.DataFrame()

def get_runner_history(con, name_query):
    """
    Finds history for a specific runner.
    """
    try:
        # Use parameter binding for safety
        query = f"""
            SELECT "Event Date", "Event Name", "Race Type", "Time", "Pace", "Overall Rank"
            FROM results_enriched
            WHERE "Name" ILIKE '%{name_query}%'
            ORDER BY "Event Date" DESC
        """
        return con.execute(query).df()
    except Exception as e:
        print(f"Error getting runner history: {e}")
        return pd.DataFrame()
