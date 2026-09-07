"""Race-day printed program theme: palette, fonts, global CSS, chart styling."""
import streamlit as st

PALETTE = {
    "paper": "#F6EFE3",
    "paper2": "#EFE5D3",
    "ink": "#1B1A17",
    "muted": "#6B6257",
    "rule": "#D9CDB8",
    "accent": "#C8501B",
    "accent2": "#2F5D50",
    "highlight": "#E0A526",
}

SERIES = [PALETTE["ink"], PALETTE["accent"], PALETTE["accent2"], PALETTE["highlight"], PALETTE["muted"]]

FONT_DISPLAY = "'Barlow Condensed', Impact, 'Arial Narrow', sans-serif"
FONT_BODY = "'IBM Plex Sans', system-ui, sans-serif"
FONT_MONO = "'IBM Plex Mono', ui-monospace, Menlo, monospace"

_CSS = f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@600;700&family=IBM+Plex+Mono:wght@500&family=IBM+Plex+Sans:wght@400;600&display=swap');

html, body, [class*="css"], .stMarkdown, .stDataFrame, .stTextInput, .stSelectbox, .stRadio, .stSlider {{
    font-family: {FONT_BODY};
    color: {PALETTE["ink"]};
}}
h1, h2, h3, h4 {{
    font-family: {FONT_DISPLAY} !important;
    font-weight: 700 !important;
    letter-spacing: 0.01em;
    color: {PALETTE["ink"]};
}}

/* Masthead */
.rp-kicker {{
    font-family: {FONT_MONO};
    font-size: 0.75rem;
    letter-spacing: 0.22em;
    text-transform: uppercase;
    color: {PALETTE["accent"]};
    margin-bottom: 0.25rem;
}}
.rp-masthead-title {{
    font-family: {FONT_DISPLAY};
    font-size: 4rem;
    line-height: 0.95;
    font-weight: 700;
    color: {PALETTE["ink"]};
    margin: 0;
}}
.rp-dateline {{
    font-family: {FONT_MONO};
    font-size: 0.85rem;
    color: {PALETTE["muted"]};
    margin-top: 0.5rem;
}}
.rp-masthead-rule {{
    border: 0;
    border-top: 3px solid {PALETTE["ink"]};
    margin: 1rem 0 0.5rem 0;
}}
.rp-intro {{
    font-size: 1.05rem;
    line-height: 1.5;
    max-width: 62ch;
    color: {PALETTE["ink"]};
    margin: 0.75rem 0 1.5rem 0;
}}

/* Section headers */
.rp-section {{ margin-top: 2rem; margin-bottom: 0.75rem; }}
.rp-section-title {{
    font-family: {FONT_DISPLAY};
    font-size: 2rem;
    font-weight: 700;
    margin: 0;
    line-height: 1.05;
}}
.rp-section-rule {{
    border: 0;
    border-top: 1px solid {PALETTE["rule"]};
    margin: 0.5rem 0 0 0;
}}

/* Bib-style stat tiles */
.rp-tile {{
    background: {PALETTE["paper2"]};
    border: 2px solid {PALETTE["ink"]};
    padding: 0.9rem 1rem 0.8rem 1rem;
    min-height: 118px;
}}
.rp-tile-label {{
    font-family: {FONT_MONO};
    font-size: 0.7rem;
    letter-spacing: 0.16em;
    text-transform: uppercase;
    color: {PALETTE["muted"]};
}}
.rp-tile-value {{
    font-family: {FONT_DISPLAY};
    font-size: 3rem;
    font-weight: 700;
    line-height: 1;
    margin: 0.35rem 0 0.2rem 0;
    color: {PALETTE["ink"]};
}}
.rp-tile-note {{
    font-size: 0.85rem;
    color: {PALETTE["muted"]};
}}

/* Verdict callout */
.rp-verdict {{
    border-left: 4px solid {PALETTE["accent"]};
    padding: 0.5rem 0.9rem;
    margin: 0.5rem 0 1rem 0;
    font-size: 1.05rem;
    background: {PALETTE["paper2"]};
}}

/* Tabs: uppercase condensed, accent underline, no pill */
.stTabs [data-baseweb="tab-list"] {{ gap: 1.5rem; border-bottom: 1px solid {PALETTE["rule"]}; }}
.stTabs [data-baseweb="tab"] {{
    font-family: {FONT_DISPLAY};
    font-size: 1.15rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    background: transparent;
    padding: 0.4rem 0;
}}
.stTabs [aria-selected="true"] {{ border-bottom: 3px solid {PALETTE["accent"]}; color: {PALETTE["accent"]}; }}

/* Sidebar */
section[data-testid="stSidebar"] {{ background: {PALETTE["paper2"]}; border-right: 1px solid {PALETTE["rule"]}; }}
</style>
"""


def inject():
    """Writes the global CSS. Call once at the top of app.py."""
    st.markdown(_CSS, unsafe_allow_html=True)


def style_chart(fig):
    """Applies the program look to a Plotly figure and returns it."""
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="IBM Plex Sans", color=PALETTE["ink"]),
        title_font=dict(family="Barlow Condensed", size=22, color=PALETTE["ink"]),
        colorway=SERIES,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                    font=dict(family="IBM Plex Mono", size=11)),
        margin=dict(l=10, r=10, t=50, b=10),
        dragmode=False,
    )
    fig.update_xaxes(showgrid=False, showline=True, linecolor=PALETTE["ink"], zeroline=False,
                     tickfont=dict(family="IBM Plex Mono", size=11), fixedrange=True)
    fig.update_yaxes(showgrid=True, gridcolor=PALETTE["rule"], gridwidth=1, zeroline=False,
                     tickfont=dict(family="IBM Plex Mono", size=11), fixedrange=True)
    return fig
