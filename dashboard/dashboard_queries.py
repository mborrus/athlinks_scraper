import duckdb
import pandas as pd

# Dashboard Queries Module
def init_db(uploaded_files):
    """
    Initializes an in-memory DuckDB connection and loads CSV files.
    Returns the connection object.
    """
    con = duckdb.connect(database=':memory:')
    
    # Create a list to hold all dataframes
    dfs = []
    
    import re

    def extract_master_id_from_filename(filename):
        match = re.search(r'scraped_(\d+)_', filename)
        if match:
            return match.group(1)
        return None

    for uploaded_file in uploaded_files:
        try:
            df = pd.read_csv(uploaded_file)
            # Ensure column names are consistent/clean
            df.columns = [c.strip() for c in df.columns]
            
            # Try to get Master ID from filename
            master_id = extract_master_id_from_filename(uploaded_file.name)
            if master_id:
                df['Master ID'] = master_id
            else:
                df['Master ID'] = None
                
            dfs.append(df)
        except Exception as e:
            print(f"Error loading {uploaded_file.name}: {e}")

    # Load local files from data directory
    import os
    # Use absolute path relative to this file
    data_dir = os.path.join(os.path.dirname(__file__), "data")
    if os.path.exists(data_dir):
        for filename in os.listdir(data_dir):
            if filename.endswith(".parquet") or filename.endswith(".csv"):
                try:
                    file_path = os.path.join(data_dir, filename)
                    if filename.endswith(".parquet"):
                        df = pd.read_parquet(file_path)
                    else:
                        df = pd.read_csv(file_path)
                    
                    df.columns = [c.strip() for c in df.columns]
                    
                    # Try to get Master ID from filename
                    master_id = extract_master_id_from_filename(filename)
                    if master_id:
                        df['Master ID'] = master_id
                    else:
                        df['Master ID'] = None
                        
                    dfs.append(df)
                except Exception as e:
                    print(f"Error loading local file {filename}: {e}")
            
    if not dfs:
        # Create empty DataFrame with expected columns to prevent Catalog Error
        columns = [
            "Event ID", "Event Name", "Event Date", "Race Type", "Name", "Gender", "Age", 
            "Bib", "City", "State", "Country", "Time", "Pace", "Overall Rank", 
            "Gender Rank", "Division Rank", "Status", "Master ID"
        ]
        full_df = pd.DataFrame(columns=columns)
    else:
        # Concatenate all dataframes
        full_df = pd.concat(dfs, ignore_index=True)
        
    # Register as a DuckDB table
    # Register as a DuckDB table
    con.register('results', full_df)
    
    return con


import json
import os

def get_metadata_path():
    return os.path.join(os.path.dirname(__file__), "data", "event_metadata.json")

def load_event_metadata():
    path = get_metadata_path()
    if os.path.exists(path):
        try:
            with open(path, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_custom_event_name(master_id, new_name):
    metadata = load_event_metadata()
    metadata[str(master_id)] = new_name.strip()
    
    path = get_metadata_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        json.dump(metadata, f, indent=2)

def get_event_names(con):
    """
    Returns a list of dictionaries with 'master_id' and 'display_name'.
    Groups by Master ID and picks the most recent Event Name, 
    overridden by custom names if available.
    """
    try:
        # Get distinct Master IDs and their most recent Event Name
        query = """
            SELECT 
                "Master ID" as master_id,
                FIRST("Event Name") as display_name
            FROM results 
            WHERE "Master ID" IS NOT NULL
            GROUP BY "Master ID"
            ORDER BY display_name ASC
        """
        df = con.execute(query).df()
        events = df.to_dict('records')
        
        # Apply custom overrides
        metadata = load_event_metadata()
        for event in events:
            mid = str(event['master_id'])
            if mid in metadata:
                event['display_name'] = metadata[mid]
                
        return events
    except Exception as e:
        print(f"Error getting event names: {e}")
        return []

def create_enriched_view(con, selected_master_id=None):
    """
    Creates or replaces the results_enriched view.
    If selected_master_id is provided, filters data to that Master ID.
    """
    
    # Base filter conditions
    where_clause = """
        "Pace" IS NOT NULL AND "Pace" != ''
        AND "Time" IS NOT NULL
        
        -- Filter out anyone faster than 12:00 (720 seconds).
        AND (
            CASE 
                WHEN "Time" LIKE '%:%:%' THEN 
                    TRY_CAST(SPLIT_PART("Time", ':', 1) AS INTEGER) * 3600 + 
                    TRY_CAST(SPLIT_PART("Time", ':', 2) AS INTEGER) * 60 + 
                    TRY_CAST(SPLIT_PART("Time", ':', 3) AS INTEGER)
                ELSE 
                    TRY_CAST(SPLIT_PART("Time", ':', 1) AS INTEGER) * 60 + 
                    TRY_CAST(SPLIT_PART("Time", ':', 2) AS INTEGER)
            END
        ) > 720
        -- Exclude DNF (Did Not Finish)
        AND ("Status" IS NULL OR "Status" != 'DNF')
    """
    
    # Add Master ID Filter if selected
    if selected_master_id:
        where_clause += f" AND \"Master ID\" = '{selected_master_id}'"

    query = f"""
        CREATE OR REPLACE VIEW results_enriched AS
        SELECT *,
             -- 1. Parse Pace to Seconds
             CASE 
                WHEN "Pace" LIKE '%:%:%' THEN 
                    TRY_CAST(SPLIT_PART("Pace", ':', 1) AS INTEGER) * 3600 + 
                    TRY_CAST(SPLIT_PART("Pace", ':', 2) AS INTEGER) * 60 + 
                    TRY_CAST(SPLIT_PART("Pace", ':', 3) AS INTEGER)
                ELSE 
                    TRY_CAST(SPLIT_PART("Pace", ':', 1) AS INTEGER) * 60 + 
                    TRY_CAST(SPLIT_PART("Pace", ':', 2) AS INTEGER)
             END as pace_seconds,

             -- 2. Parse Time to Seconds
             CASE 
                WHEN "Time" LIKE '%:%:%' THEN 
                    TRY_CAST(SPLIT_PART("Time", ':', 1) AS INTEGER) * 3600 + 
                    TRY_CAST(SPLIT_PART("Time", ':', 2) AS INTEGER) * 60 + 
                    TRY_CAST(SPLIT_PART("Time", ':', 3) AS INTEGER)
                ELSE 
                    TRY_CAST(SPLIT_PART("Time", ':', 1) AS INTEGER) * 60 + 
                    TRY_CAST(SPLIT_PART("Time", ':', 2) AS INTEGER)
             END as time_seconds,
             
             YEAR(CAST("Event Date" AS DATE)) as event_year,
             
             -- 3. Normalize Name
             CASE 
                WHEN TRIM(UPPER("Name")) = 'NESBITT DREW' THEN 'DREW NESBITT'
                ELSE TRIM(UPPER("Name"))
             END as "Name_Normalized",

             -- 4. Normalize Race Type (Catch variations of 5k and 5 Mile)
             CASE 
                WHEN REGEXP_MATCHES("Race Type", '(?i)^(run[- ]?)?5k([- ]?run)?$') THEN '5K'
                WHEN REGEXP_MATCHES("Race Type", '(?i)^(run[- ]?)?5[- ]?mil(e|er)([- ]?run)?$') THEN '5 Mile'
                ELSE "Race Type"
             END as "Race Type Normalized"

        FROM results
        WHERE {where_clause}
    """
    
    con.execute(query)

def get_overview_stats(con):
    """
    Returns basic stats: Total Runners, Avg Time, Fastest Time, and Fastest Runner Name.
    """
    try:
        query = """
            WITH primary_race AS (
                SELECT "Race Type Normalized" 
                FROM results_enriched 
                GROUP BY "Race Type Normalized" 
                ORDER BY COUNT(*) DESC 
                LIMIT 1
            )
            SELECT 
                COUNT(*) as total_runners,
                AVG(pace_seconds) as avg_pace_seconds,
                (SELECT "Time" FROM results_enriched WHERE "Race Type Normalized" = (SELECT * FROM primary_race) ORDER BY time_seconds ASC LIMIT 1) as fastest_time,
                (SELECT "Name" FROM results_enriched WHERE "Race Type Normalized" = (SELECT * FROM primary_race) ORDER BY time_seconds ASC LIMIT 1) as fastest_runner,
                (SELECT "Time" FROM results_enriched WHERE "Race Type Normalized" = (SELECT * FROM primary_race) ORDER BY time_seconds DESC LIMIT 1) as slowest_time
            FROM results_enriched
            WHERE "Race Type Normalized" = (SELECT * FROM primary_race)
        """
        return con.execute(query).df()
    except Exception:
        return pd.DataFrame()

def get_pace_partners(con, target_str, tolerance_seconds=10, search_type="Pace"):
    """
    Finds runners who finish near the target pace or time.
    target_str: "MM:SS" or "HH:MM:SS"
    tolerance_seconds: +/- seconds to include
    search_type: "Pace" or "Finish Time"
    """
    try:
        # Robust parsing of target string
        parts = target_str.split(':')
        if len(parts) == 3:
            h, m, s = map(int, parts)
            target_seconds = h * 3600 + m * 60 + s
        elif len(parts) == 2:
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
        
        column_to_filter = "pace_seconds" if search_type == "Pace" else "time_seconds"
        
        query = f"""
            SELECT "Name", "Pace", "Time", "Event Date", "Race Type"
            FROM results_enriched
            WHERE {column_to_filter} BETWEEN {min_sec} AND {max_sec}
            {year_clause}
            ORDER BY ABS({column_to_filter} - {target_seconds}) ASC
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
            SELECT "Name", COUNT(DISTINCT event_year) as race_count, MIN("Pace") as best_pace
            FROM results_enriched
            GROUP BY "Name_Normalized", "Name"
            HAVING COUNT(DISTINCT event_year) > 1
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
                MIN(pace_seconds) as min_pace_seconds,
                QUANTILE_CONT(pace_seconds, 0.95) as p95_pace_seconds,
                MEDIAN(pace_seconds) as median_pace_seconds,
                MIN(time_seconds) as min_time_seconds,
                QUANTILE_CONT(time_seconds, 0.95) as p95_time_seconds,
                MEDIAN(time_seconds) as median_time_seconds
            FROM results_enriched
            WHERE event_year IS NOT NULL 
              AND "Race Type Normalized" = (SELECT "Race Type Normalized" FROM results_enriched GROUP BY "Race Type Normalized" ORDER BY COUNT(*) DESC LIMIT 1)
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
        # Use parameter binding for safety
        query = """
            SELECT "Event Date", "Event Name", "Race Type", "Time", "Pace", "Overall Rank", time_seconds
            FROM results_enriched
            WHERE "Name" ILIKE ?
            ORDER BY "Event Date" DESC
        """
        return con.execute(query, [f'%{name_query}%']).df()
    except Exception as e:
        print(f"Error getting runner history: {e}")
        return pd.DataFrame()

def get_nemesis(con, runner_name):
    """
    Finds rivals who have raced against the target runner multiple times.
    """
    try:
        # Normalize input name for search
        runner_name_norm = runner_name.strip().upper()
        
        query = """
            WITH target_races AS (
                SELECT "Event Date", "Event Name", event_year, time_seconds
                FROM results_enriched
                WHERE "Name_Normalized" = ?
            )
            SELECT 
                r.Name as Rival,
                COUNT(*) as HeadToHead_Count,
                AVG(r.time_seconds - t.time_seconds) as Avg_Time_Diff_Seconds
            FROM results_enriched r
            JOIN target_races t ON r.event_year = t.event_year 
                AND r."Event Name" = t."Event Name"
            WHERE r."Name_Normalized" != ?
            GROUP BY Rival
            HAVING count(*) > 1
            ORDER BY HeadToHead_Count DESC, ABS(Avg_Time_Diff_Seconds) ASC
            LIMIT 20
        """
        return con.execute(query, [runner_name_norm, runner_name_norm]).df()
    except Exception as e:
        print(f"Error finding nemesis: {e}")
        return pd.DataFrame()

def get_retention_data(con):
    """
    Calculates retention flow between years for Sankey diagram.
    """
    try:
        # Get all years
        years_df = con.execute("SELECT DISTINCT event_year FROM results_enriched ORDER BY event_year").df()
        years = years_df['event_year'].tolist()
        
        if len(years) < 2:
            return []

        sankey_data = []
        
        for i in range(len(years) - 1):
            year_current = years[i]
            year_next = years[i+1]
            
            # Get runners in current year
            current_runners = con.execute(f"SELECT Name_Normalized FROM results_enriched WHERE event_year = {year_current}").df()['Name_Normalized'].tolist()
            current_set = set(current_runners)
            
            # Get runners in next year
            next_runners = con.execute(f"SELECT Name_Normalized FROM results_enriched WHERE event_year = {year_next}").df()['Name_Normalized'].tolist()
            next_set = set(next_runners)
            
            # Calculate flow
            retained = len(current_set.intersection(next_set))
            churned = len(current_set) - retained
            new_runners = len(next_set) - retained
            
            # Source, Target, Value, Label
            # 1. Retained: Year X -> Year X+1
            sankey_data.append({
                "source": str(year_current),
                "target": str(year_next),
                "value": retained,
                "type": "Retained"
            })
            
            # 2. Churned: Year X -> Churned (did not go to X+1)
            sankey_data.append({
                "source": str(year_current),
                "target": f"Left after {year_current}",
                "value": churned,
                "type": "Churned"
            })
            
            # 3. New: New -> Year X+1
            sankey_data.append({
                "source": f"New in {year_next}",
                "target": str(year_next),
                "value": new_runners,
                "type": "New"
            })
            
        return sankey_data
    except Exception as e:
        print(f"Error getting retention data: {e}")
        return []

def get_fastest_by_year(con):
    """
    Returns the fastest runner for each year (5K only).
    """
    try:
        query = """
            WITH primary_race AS (
                SELECT "Race Type Normalized" FROM results_enriched GROUP BY "Race Type Normalized" ORDER BY COUNT(*) DESC LIMIT 1
            ),
            ranked AS (
                SELECT 
                    event_year,
                    "Name",
                    "Time",
                    "Pace",
                    "Age",
                    "Gender",
                    ROW_NUMBER() OVER (PARTITION BY event_year ORDER BY time_seconds ASC) as rn
                FROM results_enriched
                WHERE "Race Type Normalized" = (SELECT * FROM primary_race)
            )
            SELECT * FROM ranked WHERE rn = 1 ORDER BY event_year DESC
        """
        return con.execute(query).df()
    except Exception as e:
        print(f"Error getting fastest by year: {e}")
        return pd.DataFrame()

def get_fastest_by_demographics(con):
    """
    Returns fastest time by Gender and Age Group (5K only).
    """
    try:
        query = """
            WITH primary_race AS (
                SELECT "Race Type Normalized" FROM results_enriched GROUP BY "Race Type Normalized" ORDER BY COUNT(*) DESC LIMIT 1
            ),
            age_grouped AS (
                SELECT *,
                    CASE 
                        WHEN "Age" < 15 THEN '0-14'
                        WHEN "Age" BETWEEN 15 AND 19 THEN '15-19'
                        WHEN "Age" BETWEEN 20 AND 29 THEN '20-29'
                        WHEN "Age" BETWEEN 30 AND 39 THEN '30-39'
                        WHEN "Age" BETWEEN 40 AND 49 THEN '40-49'
                        WHEN "Age" BETWEEN 50 AND 59 THEN '50-59'
                        WHEN "Age" BETWEEN 60 AND 69 THEN '60-69'
                        WHEN "Age" >= 70 THEN '70+'
                        ELSE 'Unknown'
                    END as Age_Group
                FROM results_enriched
                WHERE "Race Type Normalized" = (SELECT * FROM primary_race) AND "Age" IS NOT NULL
            ),
            ranked AS (
                SELECT 
                    Age_Group,
                    "Gender",
                    "Name",
                    "Time",
                    "Pace",
                    event_year,
                    ROW_NUMBER() OVER (PARTITION BY Age_Group, "Gender" ORDER BY time_seconds ASC) as rn
                FROM age_grouped
            )
            SELECT * FROM ranked WHERE rn = 1 ORDER BY "Gender", Age_Group
        """
        return con.execute(query).df()
    except Exception as e:
        print(f"Error getting fastest by demographics: {e}")
        return pd.DataFrame()

def get_division_stats(con):
    """
    Analyzes competitiveness of age divisions (5K only).
    """
    try:
        query = """
            WITH primary_race AS (
                SELECT "Race Type Normalized" FROM results_enriched GROUP BY "Race Type Normalized" ORDER BY COUNT(*) DESC LIMIT 1
            ),
            age_grouped AS (
                SELECT *,
                    CASE 
                        WHEN "Age" < 15 THEN '0-14'
                        WHEN "Age" BETWEEN 15 AND 19 THEN '15-19'
                        WHEN "Age" BETWEEN 20 AND 29 THEN '20-29'
                        WHEN "Age" BETWEEN 30 AND 39 THEN '30-39'
                        WHEN "Age" BETWEEN 40 AND 49 THEN '40-49'
                        WHEN "Age" BETWEEN 50 AND 59 THEN '50-59'
                        WHEN "Age" BETWEEN 60 AND 69 THEN '60-69'
                        WHEN "Age" >= 70 THEN '70+'
                        ELSE 'Unknown'
                    END as Age_Group
                FROM results_enriched
                WHERE "Race Type Normalized" = (SELECT * FROM primary_race) AND "Age" IS NOT NULL
            ),
            div_stats AS (
                SELECT 
                    Age_Group,
                    COUNT(*) as runner_count,
                    AVG(pace_seconds) as avg_pace_seconds,
                    -- Calculate spread between 1st and 3rd place
                    (
                        SELECT time_seconds 
                        FROM age_grouped t2 
                        WHERE t2.Age_Group = t1.Age_Group 
                        ORDER BY time_seconds ASC 
                        LIMIT 1 OFFSET 2
                    ) - (
                        SELECT time_seconds 
                        FROM age_grouped t2 
                        WHERE t2.Age_Group = t1.Age_Group 
                        ORDER BY time_seconds ASC 
                        LIMIT 1
                    ) as top_3_spread_seconds
                FROM age_grouped t1
                GROUP BY Age_Group
            )
            SELECT * FROM div_stats ORDER BY runner_count DESC
        """
        return con.execute(query).df()
    except Exception as e:
        print(f"Error getting division stats: {e}")
        return pd.DataFrame()

def get_era_stats(con):
    """
    Compares performance between 5-year eras (e.g., 2010-2014, 2015-2019) (5K only).
    """
    try:
        query = """
            WITH primary_race AS (
                SELECT "Race Type Normalized" FROM results_enriched GROUP BY "Race Type Normalized" ORDER BY COUNT(*) DESC LIMIT 1
            )
            SELECT 
                CAST(FLOOR(event_year / 5) * 5 AS INTEGER) as Era_Start,
                COUNT(*) / COUNT(DISTINCT event_year) as avg_runners_per_year,
                AVG(pace_seconds) as avg_pace_seconds,
                MIN(time_seconds) as fastest_time_seconds
            FROM results_enriched
            WHERE "Race Type Normalized" = (SELECT * FROM primary_race)
            GROUP BY Era_Start
            ORDER BY Era_Start
        """
        return con.execute(query).df()
    except Exception as e:
        print(f"Error getting era stats: {e}")
        return pd.DataFrame()

def get_raw_times(con):
    """
    Returns all finish times in seconds for 5K races.
    """
    try:
        query = """
            WITH primary_race AS (
                SELECT "Race Type Normalized" FROM results_enriched GROUP BY "Race Type Normalized" ORDER BY COUNT(*) DESC LIMIT 1
            )
            SELECT time_seconds
            FROM results_enriched
            WHERE "Race Type Normalized" = (SELECT * FROM primary_race) AND time_seconds IS NOT NULL
        """
        return con.execute(query).df()
    except Exception as e:
        print(f"Error getting raw times: {e}")
        return pd.DataFrame()

def get_avg_annual_runners(con):
    """
    Returns the average number of runners per year (5K only).
    """
    try:
        query = """
            WITH primary_race AS (
                SELECT "Race Type Normalized" FROM results_enriched GROUP BY "Race Type Normalized" ORDER BY COUNT(*) DESC LIMIT 1
            )
            SELECT COUNT(*) * 1.0 / COUNT(DISTINCT event_year) as avg_runners
            FROM results_enriched
            WHERE "Race Type Normalized" = (SELECT * FROM primary_race)
        """
        result = con.execute(query).fetchone()
        return result[0] if result else 0
    except Exception as e:
        print(f"Error getting avg annual runners: {e}")
        return 0
