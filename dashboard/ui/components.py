"""Reusable render helpers. Every HTML string in the dashboard lives here or in theme.py."""
import html

import streamlit as st

from ui import theme


def _esc(value):
    return html.escape("" if value is None else str(value))


def masthead(title, dateline, intro=None):
    st.markdown(
        f"""
        <div class="rp-kicker">Official results program</div>
        <div class="rp-masthead-title">{_esc(title)}</div>
        <div class="rp-dateline">{_esc(dateline)}</div>
        <hr class="rp-masthead-rule"/>
        """,
        unsafe_allow_html=True,
    )
    if intro:
        st.markdown(f'<div class="rp-intro">{_esc(intro)}</div>', unsafe_allow_html=True)


def section(title, kicker=None):
    kicker_html = f'<div class="rp-kicker">{_esc(kicker)}</div>' if kicker else ""
    st.markdown(
        f"""
        <div class="rp-section">
            {kicker_html}
            <div class="rp-section-title">{_esc(title)}</div>
            <hr class="rp-section-rule"/>
        </div>
        """,
        unsafe_allow_html=True,
    )


def stat_tile(label, value, note=None):
    note_html = f'<div class="rp-tile-note">{_esc(note)}</div>' if note else ""
    st.markdown(
        f"""
        <div class="rp-tile">
            <div class="rp-tile-label">{_esc(label)}</div>
            <div class="rp-tile-value">{_esc(value)}</div>
            {note_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def stat_row(items):
    """items: list of (label, value, note) tuples, 2-4 long."""
    cols = st.columns(len(items))
    for col, (label, value, note) in zip(cols, items):
        with col:
            stat_tile(label, value, note)


def verdict(text):
    st.markdown(f'<div class="rp-verdict">{_esc(text)}</div>', unsafe_allow_html=True)


def chart(fig):
    st.plotly_chart(theme.style_chart(fig), use_container_width=True,
                    config={"displayModeBar": False, "scrollZoom": False})


def table(df):
    st.dataframe(df, use_container_width=True, hide_index=True)
