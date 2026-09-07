import math
import re

import duckdb
import pandas as pd

import storage
from athlinks_scraper.providers.naming import race_group_label
from storage import backfill_legacy_columns, extract_master_id_from_filename  # noqa: F401  (re-exported)

# Dashboard Queries Module


def format_seconds(total_seconds):
    """
    1500 -> "25:00"; 3725 -> "1:02:05"; None or NaN -> "N/A".
    Used wherever the UI shows a duration computed in SQL.
    """
    if total_seconds is None or (isinstance(total_seconds, float) and math.isnan(total_seconds)):
        return "N/A"
    total_seconds = int(total_seconds)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    return f"{minutes}:{seconds:02d}"


def init_db_from_dataframe(df):
    """
    Creates an in-memory DuckDB connection with `df` registered as the
    `results` table. This is the seam that tests use.
    """
    con = duckdb.connect(database=':memory:')
    con.register('results', df)
    return con


def get_event_names(con):
    """
    One entry per Race Group: {'group_key', 'display_name', 'source_name', 'n_years'}.
    display_name is the year-stripped event name, overridden by the
    event_metadata table when present. `con` may be the durable store or a
    plain in-memory connection that only has `results`.
    """
    df = storage.list_groups(con)
    overrides = storage.load_event_metadata(con)
    groups = []
    for rec in df.to_dict('records'):
        key = str(rec["group_key"])
        groups.append({
            "group_key": key,
            "display_name": overrides.get(key) or race_group_label(rec["event_name"] or key),
            "source_name": rec["source_name"],
            "n_years": int(rec["n_years"]),
        })
    groups.sort(key=lambda g: g["display_name"].lower())
    return groups


def create_enriched_view(con, selected_group=None):
    """
    Creates or replaces the results_enriched view with parsed seconds, year,
    normalized name and normalized race type. Drops DNFs and impossible times.
    If selected_group is given, only that Race Group is included.

    DuckDB cannot bind parameters inside CREATE VIEW, so the group key is
    validated against a strict slug pattern instead.
    """
    con.execute("""
        CREATE OR REPLACE MACRO to_seconds(txt) AS CAST(
            CASE
                WHEN txt LIKE '%:%:%' THEN
                    TRY_CAST(SPLIT_PART(txt, ':', 1) AS DOUBLE) * 3600 +
                    TRY_CAST(SPLIT_PART(txt, ':', 2) AS DOUBLE) * 60 +
                    TRY_CAST(SPLIT_PART(txt, ':', 3) AS DOUBLE)
                WHEN txt LIKE '%:%' THEN
                    TRY_CAST(SPLIT_PART(txt, ':', 1) AS DOUBLE) * 60 +
                    TRY_CAST(SPLIT_PART(txt, ':', 2) AS DOUBLE)
                ELSE NULL
            END AS INTEGER)
    """)

    where_clause = """
        "Pace" IS NOT NULL AND "Pace" != ''
        AND "Time" IS NOT NULL
        -- Anyone faster than 12:00 (720 s) is a timing error, not a finisher.
        AND to_seconds("Time") > 720
        AND ("Status" IS NULL OR "Status" != 'DNF')
    """

    if selected_group is not None:
        key = str(selected_group)
        if not re.fullmatch(storage.GROUP_KEY_PATTERN, key):
            raise ValueError(f"Race Group key contains unsafe characters: {selected_group!r}")
        where_clause += f" AND \"Race Group\" = '{key}'"

    con.execute(f"""
        CREATE OR REPLACE VIEW results_enriched AS
        SELECT *,
             to_seconds("Pace") as pace_seconds,
             to_seconds("Time") as time_seconds,
             YEAR(TRY_CAST("Event Date" AS DATE)) as event_year,
             CASE
                WHEN TRIM(UPPER("Name")) = 'NESBITT DREW' THEN 'DREW NESBITT'
                ELSE TRIM(UPPER("Name"))
             END as "Name_Normalized",
             CASE
                WHEN REGEXP_MATCHES("Race Type", '(?i)^(run[- ]?)?5k([- ]?(run|walk|run/walk))?$') THEN '5K'
                WHEN REGEXP_MATCHES("Race Type", '(?i)^(run[- ]?)?5[- ]?mil(e|er)([- ]?run)?$') THEN '5 Mile'
                ELSE "Race Type"
             END as "Race Type Normalized"
        FROM results
        WHERE {where_clause}
    """)

def get_overview_stats(con):
    """
    Headline numbers for the primary race type: total finishers, average pace,
    fastest/slowest time, who set the fastest time and in which year, and the
    first year of data.
    """
    try:
        query = """
            WITH primary_race AS (
                SELECT "Race Type Normalized" FROM results_enriched
                GROUP BY "Race Type Normalized" ORDER BY COUNT(*) DESC LIMIT 1
            ),
            primary_rows AS (
                SELECT * FROM results_enriched
                WHERE "Race Type Normalized" = (SELECT * FROM primary_race)
            ),
            fastest AS (
                SELECT "Time", "Name", event_year
                FROM primary_rows ORDER BY time_seconds ASC LIMIT 1
            ),
            slowest AS (
                SELECT "Time" FROM primary_rows ORDER BY time_seconds DESC LIMIT 1
            )
            SELECT
                COUNT(*) as total_runners,
                AVG(pace_seconds) as avg_pace_seconds,
                (SELECT "Time" FROM fastest) as fastest_time,
                (SELECT "Name" FROM fastest) as fastest_runner,
                (SELECT event_year FROM fastest) as fastest_year,
                MIN(event_year) as first_year,
                (SELECT "Time" FROM slowest) as slowest_time
            FROM primary_rows
        """
        return con.execute(query).df()
    except Exception as e:
        print(f"Error getting overview stats: {e}")
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
    "Frequent Flyers": runners who appear in more than one year, with their
    best (lowest) pace. Pace is compared numerically via pace_seconds — the
    "Pace" column is a string and would sort "10:00" before "9:40".
    """
    try:
        df = con.execute("""
            SELECT
                "Name",
                COUNT(DISTINCT event_year) as race_count,
                MIN(pace_seconds) as best_pace_seconds
            FROM results_enriched
            WHERE pace_seconds IS NOT NULL
            GROUP BY "Name_Normalized", "Name"
            HAVING COUNT(DISTINCT event_year) > 1
            ORDER BY race_count DESC, best_pace_seconds ASC
            LIMIT 10
        """).df()
        df["best_pace"] = df["best_pace_seconds"].apply(format_seconds)
        return df[["Name", "race_count", "best_pace"]]
    except Exception as e:
        print(f"Error getting fun stats: {e}")
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
            SELECT time_seconds, "Gender", "Age", event_year
            FROM results_enriched
            WHERE "Race Type Normalized" = (SELECT * FROM primary_race) AND time_seconds IS NOT NULL
        """
        return con.execute(query).df()
    except Exception as e:
        print(f"Error getting raw times: {e}")
        return pd.DataFrame()

def get_competitiveness_stats(con, gender="All", age_min=0, age_max=100):
    """
    Returns the 3rd and 10th place finish times (seconds) by year for the
    primary race, filtered by gender ("All", "M" or "F") and age range.
    """
    try:
        params = [age_min, age_max]
        gender_clause = ""
        if gender != "All":
            gender_clause = 'AND "Gender" = ?'
            params.append(gender)

        query = f"""
            WITH primary_race AS (
                SELECT "Race Type Normalized" FROM results_enriched
                GROUP BY "Race Type Normalized" ORDER BY COUNT(*) DESC LIMIT 1
            ),
            ranked AS (
                SELECT
                    event_year,
                    time_seconds,
                    ROW_NUMBER() OVER (PARTITION BY event_year ORDER BY time_seconds ASC) as rn
                FROM results_enriched
                WHERE "Race Type Normalized" = (SELECT * FROM primary_race)
                  AND "Age" BETWEEN ? AND ?
                  {gender_clause}
            )
            SELECT
                event_year,
                MAX(CASE WHEN rn = 3 THEN time_seconds END) as time_top_3,
                MAX(CASE WHEN rn = 10 THEN time_seconds END) as time_top_10
            FROM ranked
            WHERE rn IN (3, 10)
            GROUP BY event_year
            ORDER BY event_year
        """
        return con.execute(query, params).df()
    except Exception as e:
        print(f"Error getting competitiveness stats: {e}")
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


# --- Feature queries ------------------------------------------------------

_PRIMARY_RACE = """(
    SELECT "Race Type Normalized" FROM results_enriched
    GROUP BY "Race Type Normalized" ORDER BY COUNT(*) DESC LIMIT 1
)"""


def search_runner_names(con, fragment):
    """Distinct normalized names containing `fragment` (case-insensitive), sorted."""
    try:
        df = con.execute(
            'SELECT "Name_Normalized" FROM results_enriched WHERE "Name_Normalized" ILIKE ? '
            'GROUP BY "Name_Normalized" ORDER BY "Name_Normalized"',
            [f"%{fragment}%"],
        ).df()
        return df["Name_Normalized"].tolist()
    except Exception as e:
        print(f"Error searching names: {e}")
        return []


def get_runner_yearly(con, name_norm):
    """
    One row per year the runner finished the primary race: their time/pace,
    computed place, field size, % of the field they beat, and the field median.
    """
    try:
        query = f"""
            WITH field AS (
                SELECT event_year, COUNT(*) AS field_size, MEDIAN(time_seconds) AS median_seconds
                FROM results_enriched
                WHERE "Race Type Normalized" = {_PRIMARY_RACE}
                GROUP BY event_year
            ),
            me AS (
                SELECT event_year, "Time", "Pace", time_seconds
                FROM results_enriched
                WHERE "Name_Normalized" = ? AND "Race Type Normalized" = {_PRIMARY_RACE}
            ),
            placed AS (
                SELECT m.*,
                    (SELECT COUNT(*) FROM results_enriched r
                     WHERE r.event_year = m.event_year
                       AND r."Race Type Normalized" = {_PRIMARY_RACE}
                       AND r.time_seconds < m.time_seconds) + 1 AS place
                FROM me m
            )
            SELECT p.event_year, p."Time", p."Pace", p.time_seconds, p.place, f.field_size,
                   ROUND(100.0 * (f.field_size - p.place) / f.field_size, 1) AS pct_beaten,
                   f.median_seconds
            FROM placed p JOIN field f USING (event_year)
            ORDER BY p.event_year
        """
        return con.execute(query, [name_norm]).df()
    except Exception as e:
        print(f"Error getting runner yearly: {e}")
        return pd.DataFrame()


def get_head_to_head(con, name_a, name_b):
    """Years both runners finished the same race type; diff_seconds = A - B."""
    try:
        query = """
            SELECT a.event_year, a."Time" AS time_a, b."Time" AS time_b,
                   a.time_seconds - b.time_seconds AS diff_seconds
            FROM results_enriched a
            JOIN results_enriched b
              ON a.event_year = b.event_year
             AND a."Race Type Normalized" = b."Race Type Normalized"
            WHERE a."Name_Normalized" = ? AND b."Name_Normalized" = ?
            ORDER BY a.event_year
        """
        return con.execute(query, [name_a, name_b]).df()
    except Exception as e:
        print(f"Error getting head to head: {e}")
        return pd.DataFrame()


def get_returning_counts(con):
    """Per year: how many finishers were new vs had raced in an earlier year."""
    try:
        query = """
            WITH first_seen AS (
                SELECT "Name_Normalized", MIN(event_year) AS first_year
                FROM results_enriched GROUP BY "Name_Normalized"
            ),
            appearances AS (
                SELECT DISTINCT "Name_Normalized", event_year FROM results_enriched
            )
            SELECT a.event_year,
                   CAST(SUM(CASE WHEN a.event_year = f.first_year THEN 1 ELSE 0 END) AS INTEGER) AS new_runners,
                   CAST(SUM(CASE WHEN a.event_year > f.first_year THEN 1 ELSE 0 END) AS INTEGER) AS returning_runners
            FROM appearances a JOIN first_seen f USING ("Name_Normalized")
            GROUP BY a.event_year
            ORDER BY a.event_year
        """
        return con.execute(query).df()
    except Exception as e:
        print(f"Error getting returning counts: {e}")
        return pd.DataFrame()
