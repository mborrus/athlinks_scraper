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
    # Built as a single joined string (no blank/whitespace-only lines): a
    # blank line inside an unsafe_allow_html block ends the HTML block early
    # per CommonMark, leaving the rest to render as a literal indented code
    # block. That happens here whenever kicker is omitted and its line would
    # otherwise be empty.
    parts = ['<div class="rp-section">']
    if kicker:
        parts.append(f'<div class="rp-kicker">{_esc(kicker)}</div>')
    parts.append(f'<div class="rp-section-title">{_esc(title)}</div>')
    parts.append('<hr class="rp-section-rule"/>')
    parts.append("</div>")
    st.markdown("\n".join(parts), unsafe_allow_html=True)


def stat_tile(label, value, note=None):
    # See the note in section() above: no blank lines when note is omitted.
    parts = [
        '<div class="rp-tile">',
        f'<div class="rp-tile-label">{_esc(label)}</div>',
        f'<div class="rp-tile-value">{_esc(value)}</div>',
    ]
    if note:
        parts.append(f'<div class="rp-tile-note">{_esc(note)}</div>')
    parts.append("</div>")
    st.markdown("\n".join(parts), unsafe_allow_html=True)


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
