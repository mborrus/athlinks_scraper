# Athlinks Race Analytics Dashboard

A powerful Streamlit-based dashboard for analyzing race results, finding pace partners, and visualizing performance trends.

## Features

- **Race Overview**: Get quick stats on total runners, average pace, and fastest/slowest times.
- **Performance Trends**: Visualize how pace metrics (min, max, median) evolve over the years.
- **Pace Distribution**: View a histogram of pace distribution across all runners.
- **Runner Lookup**: Search for specific runners to see their race history and performance.
- **Pace Partners**: Input your target pace to find other runners who finish near your time—perfect for finding training buddies or rivals!
- **Hall of Fame**: Discover the "Frequent Flyers" who have raced the most times.

## Installation

1.  Navigate to the dashboard directory:
    ```bash
    cd dashboard
    ```

2.  Install the required dependencies:
    ```bash
    pip install -r requirements.txt
    ```

## Usage

1.  Run the Streamlit app:
    ```bash
    streamlit run app.py
    ```

2.  **Upload Data**:
    - Once the app opens in your browser, use the sidebar to upload one or more race result CSV files.
    - **CSV Requirements**: The files should contain at least the following columns:
        - `Name`
        - `Time`
        - `Pace` (Format: MM:SS or HH:MM:SS)
        - `Event Date`
        - `Race Type`

3.  **Explore**: Use the various sections to analyze the data!

## Tech Stack

- **Streamlit**: For the interactive web interface.
- **DuckDB**: For fast, in-memory SQL querying of data.
- **Pandas**: For data manipulation.
- **Plotly**: For interactive charts and graphs.
