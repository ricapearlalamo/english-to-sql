# app.py
# English-to-SQL Translator (NLTK) â€” Restaurant / CafÃ© Edition
# Supports: monthly / quarterly / yearly, "January 2025", "Q4 2025", "in 2024", and explicit date between
# Works with SQLite (default) or PostgreSQL

import os
import re
import calendar
from functools import lru_cache
from datetime import date

import streamlit as st
import pandas as pd

# NLP
import nltk
from nltk import word_tokenize, pos_tag

# Database
import sqlite3

# ---- OPTIONAL Postgres import (won't crash if psycopg2 isn't installed) ----
try:
    import psycopg2  # noqa: F401
    import psycopg2.extras  # noqa: F401
except Exception:
    psycopg2 = None  # sentinel so we can check later

# ========= NLTK setup =========
@lru_cache(maxsize=1)
def _download_nltk():
    try:
        nltk.data.find("tokenizers/punkt")
    except LookupError:
        nltk.download("punkt", quiet=True)
    # Older tagger name still commonly used; if this errors you can switch to 'averaged_perceptron_tagger_eng'
    try:
        nltk.data.find("taggers/averaged_perceptron_tagger")
    except LookupError:
        nltk.download("averaged_perceptron_tagger", quiet=True)

_download_nltk()


def tokenize_and_tag(q: str):
    try:
        tokens = word_tokenize(q.lower())
        tagged = pos_tag(tokens)
        return tokens, tagged
    except LookupError:
        tokens = q.lower().split()
        tagged = [(t, "NN") for t in tokens]
        return tokens, tagged


# ========= Page =========
st.set_page_config(page_title="Cafe BI: English to SQL", layout="wide")

# Minimal styling: blue buttons + a nicer helper when disconnected
st.markdown("""
<style>\n/\*\ ---\ Unified\ Button\ Styling\ \(Main\ \+\ Sidebar\)\ ---\ \*/\n\.stButton\ >\ button,\ndiv\[data-testid="stSidebar"]\ \.stButton\ >\ button\ \{\n\ \ background:\ \#B9D9EB\ !important;\ \ \ \ /\*\ light\ blue\ \*/\n\ \ color:\ \#1f2937\ !important;\ \ \ \ \ \ \ \ \ \ /\*\ dark\ text\ \*/\n\ \ border:\ none\ !important;\n\ \ border-radius:\ 10px\ !important;\n\ \ padding:\ 8px\ 16px\ !important;\n\ \ font-weight:\ 700\ !important;\n\ \ box-shadow:\ none\ !important;\n\ \ transition:\ background-color\ 120ms\ ease,\ color\ 120ms\ ease\ !important;\n}\n/\*\ Hover\ =\ darker\ background\ \+\ light\ text\ \*/\n\.stButton\ >\ button:hover,\ndiv\[data-testid="stSidebar"]\ \.stButton\ >\ button:hover\ \{\n\ \ background:\ \#4B9CD3\ !important;\ \ \ \ \ /\*\ darker\ blue\ \*/\n\ \ color:\ \#FFFFFF\ !important;\ \ \ \ \ \ \ \ \ \ \ /\*\ light\ text\ \*/\n}\n/\*\ Active/Pressed/Focus\ =\ same\ darker\ background\ \+\ light\ text\ \(no\ red\ flash\)\ \*/\n\.stButton\ >\ button:active,\n\.stButton\ >\ button:focus,\n\.stButton\ >\ button:focus-visible,\n\.stButton\ >\ button\[aria-pressed="true"],\ndiv\[data-testid="stSidebar"]\ \.stButton\ >\ button:active,\ndiv\[data-testid="stSidebar"]\ \.stButton\ >\ button:focus,\ndiv\[data-testid="stSidebar"]\ \.stButton\ >\ button:focus-visible,\ndiv\[data-testid="stSidebar"]\ \.stButton\ >\ button\[aria-pressed="true"]\ \{\n\ \ background:\ \#3A88C5\ !important;\ \ \ \ \ /\*\ slightly\ darker\ than\ hover\ for\ pressed\ \*/\n\ \ color:\ \#FFFFFF\ !important;\ \ \ \ \ \ \ \ \ \ \ /\*\ keep\ text\ light\ \*/\n\ \ outline:\ none\ !important;\n\ \ box-shadow:\ none\ !important;\n}\n/\*\ Disabled\ state\ \*/\n\.stButton\ >\ button:disabled,\ndiv\[data-testid="stSidebar"]\ \.stButton\ >\ button:disabled\ \{\n\ \ background:\ \#E5F0F8\ !important;\ \ \ \ \ /\*\ pale\ blue\ \*/\n\ \ color:\ \#9CA3AF\ !important;\ \ \ \ \ \ \ \ \ \ /\*\ gray\ text\ \*/\n\ \ opacity:\ 1\ !important;\n}\n</style>
""", unsafe_allow_html=True)

st.title("ðŸ¥¤English-to-SQL Translator â€” Restaurant / CafÃ© Edition")
st.markdown(
    '<div class="app-hero">Ask business questions in plain English â€” get instant SQL and insights from the CafÃ© dataset.</div>',
    unsafe_allow_html=True
)


# ========= Sidebar: Connection =========
st.sidebar.header("Database Connection")
db_type = st.sidebar.selectbox("Database Type", ["SQLite", "PostgreSQL"])

# Seed paths/constants
DEFAULT_SQLITE_PATH = "retail_demo.sqlite"
SEED_FILE = "seed_sqlite.sql"

if db_type == "SQLite":
    sqlite_path = st.sidebar.text_input("SQLite File Path", value=DEFAULT_SQLITE_PATH)
else:
    pg_host = st.sidebar.text_input("Database Host (IP or localhost)", value="localhost")
    pg_port = st.sidebar.text_input("Port", value="5432")
    pg_user = st.sidebar.text_input("Username", value="postgres")
    pg_pass = st.sidebar.text_input("Password", type="password")
    pg_db   = st.sidebar.text_input("Database Name", value="postgres")

connect_btn = st.sidebar.button("Connect & Scan Schema")


# ========= DB helpers =========
def init_sqlite_if_needed(path: str, seed_sql: str = SEED_FILE):
    """Create and seed SQLite database if missing."""
    if not os.path.exists(path):
        # Only create if seed file exists
        if not os.path.exists(seed_sql):
            raise FileNotFoundError(
                f"SQLite DB '{path}' not found and seed file '{seed_sql}' is missing."
            )
        con = sqlite3.connect(path)
        with open(seed_sql, "r", encoding="utf-8") as f:
            con.executescript(f.read())
        con.commit()
        con.close()

def get_connection():
    if db_type == "SQLite":
        # Ensure DB exists and is seeded
        init_sqlite_if_needed(sqlite_path, SEED_FILE)
        return sqlite3.connect(sqlite_path, check_same_thread=False)
    else:
        if psycopg2 is None:
            # User selected Postgres but driver isn't installed
            raise RuntimeError(
                "PostgreSQL selected but psycopg2 is not installed. "
                "Either add 'psycopg2-binary==2.9.9' to requirements.txt or switch to SQLite."
            )
        return psycopg2.connect(
            host=pg_host, port=int(pg_port), user=pg_user, password=pg_pass, dbname=pg_db
        )


def get_schema(conn):
    schema = {}
    if db_type == "SQLite":
        cur = conn.cursor()
        tables = cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
        ).fetchall()
        for (tname,) in tables:
            cols = cur.execute(f"PRAGMA table_info({tname});").fetchall()
            schema[tname] = [c[1] for c in cols]
        cur.close()
    else:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name, column_name
                FROM information_schema.columns
                WHERE table_schema='public'
                ORDER BY table_name, ordinal_position;
            """)
            for tname, col in cur.fetchall():
                schema.setdefault(tname, []).append(col)
    return schema


def fetch_min_max_dates(conn):
    try:
        if db_type == "SQLite":
            sql = "SELECT MIN(order_date), MAX(order_date) FROM orders;"
            cur = conn.cursor()
            mn, mx = cur.execute(sql).fetchone()
            cur.close()
        else:
            with conn.cursor() as cur:
                cur.execute("SELECT MIN(order_date::date), MAX(order_date::date) FROM orders;")
                mn, mx = cur.fetchone()
                if isinstance(mn, (date,)): mn = mn.isoformat()
                if isinstance(mx, (date,)): mx = mx.isoformat()
        return mn, mx
    except Exception:
        return None, None


# ========= Session state =========
init_state = {"schema": None, "conn": None, "prefill": None, "date_min": None, "date_max": None}
for k, v in init_state.items():
    if k not in st.session_state:
        st.session_state[k] = v


# ========= Connect =========
if connect_btn:
    try:
        conn = get_connection()
        schema = get_schema(conn)
        date_min, date_max = fetch_min_max_dates(conn)
        st.session_state["conn"] = conn
        st.session_state["schema"] = schema
        st.session_state["date_min"] = date_min
        st.session_state["date_max"] = date_max

        st.markdown(
            f"""
            <div class="banner">
                âœ… Connected! Found {len(schema)} table(s)
            </div>
            """,
            unsafe_allow_html=True
        )
    except Exception as e:
        st.error(f"Connection/Schema error: {e}")


# ========= Translation helpers (time buckets) =========
AGG_KEYWORDS = {
    "total": "SUM", "sum": "SUM", "sales": "SUM", "revenue": "SUM",
    "average": "AVG", "avg": "AVG", "count": "COUNT", "number": "COUNT",
    "maximum": "MAX", "max": "MAX", "minimum": "MIN", "min": "MIN"
}
DISTINCT_WORDS = {"distinct", "unique"}
TOP_N_RE = re.compile(r"\b(top|best)\s+(\d+)\b")

MONTHS = {
    "january": 1, "jan": 1, "february": 2, "feb": 2, "march": 3, "mar": 3,
    "april": 4, "apr": 4, "may": 5, "june": 6, "jun": 6, "july": 7, "jul": 7,
    "august": 8, "aug": 8, "september": 9, "sep": 9, "sept": 9,
    "october": 10, "oct": 10, "november": 11, "nov": 11, "december": 12, "dec": 12
}

def wants_distinct(tokens): return any(w in DISTINCT_WORDS for w in tokens)
def tokens_contain(tokens, words): return any(w in set(tokens) for w in words)

def extract_top_n(tokens):
    q = " ".join(tokens)
    m = TOP_N_RE.search(q)
    return int(m.group(2)) if m else None

def extract_time_bucket(tokens, text):
    t = text.lower()
    if any(p in t for p in ["quarterly", "by quarter", "qtr"]) or re.search(r"\bq[1-4]\b", t) or "quarter" in tokens:
        return "quarter"
    if any(p in t for p in ["by month", "monthly", "per month", "each month", "months"]) or "month" in tokens:
        return "month"
    if any(p in t for p in ["yearly", "per year", "by year", "years"]) or "year" in tokens or "yearly" in tokens:
        return "year"
    if "day" in tokens:
        return "day"
    return None

def period_expressions(bucket: str, date_col: str = "orders.order_date"):
    if not bucket:
        return None, None, None, None
    if db_type == "SQLite":
        d = date_col
        if bucket == "day":
            label = f"strftime('%Y-%m-%d', {d})"; return label, label, label, "period"
        if bucket == "month":
            label = f"strftime('%Y-%m', {d})"; return label, label, label, "period"
        if bucket == "year":
            label = f"strftime('%Y', {d})"; return label, label, label, "period"
        if bucket == "quarter":
            q_case = (
                "CASE "
                "WHEN CAST(strftime('%m', {d}) AS INTEGER) BETWEEN 1 AND 3 THEN 1 "
                "WHEN CAST(strftime('%m', {d}) AS INTEGER) BETWEEN 4 AND 6 THEN 2 "
                "WHEN CAST(strftime('%m', {d}) AS INTEGER) BETWEEN 7 AND 9 THEN 3 "
                "ELSE 4 END"
            ).format(d=d)
            year = f"CAST(strftime('%Y', {d}) AS INTEGER)"
            label = f"strftime('%Y', {d}) || '-Q' || {q_case}"
            grp = f"{year}, {q_case}"
            ordk = f"({year}*10 + {q_case})"
            return label, grp, ordk, "period"
    else:
        if bucket == "day":
            label = f"to_char({date_col}::date, 'YYYY-MM-DD')"
            grp = f"date_trunc('day', {date_col}::timestamp)"; return label, grp, grp, "period"
        if bucket == "month":
            label = f"to_char({date_col}::date, 'YYYY-MM')"
            grp = f"date_trunc('month', {date_col}::timestamp)"; return label, grp, grp, "period"
        if bucket == "year":
            label = f"to_char({date_col}::date, 'YYYY')"
            grp = f"date_trunc('year', {date_col}::timestamp)"; return label, grp, grp, "period"
        if bucket == "quarter":
            label = f"to_char({date_col}::date, 'YYYY-\"Q\"Q')"
            grp = f"date_trunc('quarter', {date_col}::timestamp)"; return label, grp, grp, "period"
    return None, None, None, None

def sql_placeholders():
    return "%s" if db_type == "PostgreSQL" else "?"

def base_joins():
    return (
        "FROM order_items\n"
        "JOIN orders   ON order_items.order_id = orders.order_id\n"
        "LEFT JOIN products  ON order_items.product_id = products.product_id\n"
        "LEFT JOIN customers ON orders.customer_id = customers.customer_id\n"
    )

# ======== Natural-language period filters (year, month-year, quarter-year) ========
YEAR_RE = re.compile(r"\b(20\d{2})\b")
MONTH_YEAR_RE = re.compile(r"\b(" + "|".join(MONTHS.keys()) + r")\s+(20\d{2})\b", re.IGNORECASE)

def _year_condition():
    if db_type == "SQLite":
        return "CAST(strftime('%Y', orders.order_date) AS INTEGER) = " + sql_placeholders()
    else:
        return "EXTRACT(YEAR FROM orders.order_date)::INT = " + sql_placeholders()

def _between_condition():
    return "orders.order_date BETWEEN " + sql_placeholders() + " AND " + sql_placeholders()

def extract_period_filters(text: str):
    """
    Returns (where_sql_parts:list[str], params:list, inferred_bucket:str|None, single_year:int|None)
    Recognizes: Year, Month-Year, Quarter-Year (Q4 2025 / first quarter of 2024)
    """
    t = text.lower()
    where_parts, params = [], []
    inferred_bucket = None
    single_year = None

    # Month + Year (e.g., "January 2025")
    m = MONTH_YEAR_RE.search(t)
    if m:
        mon_name, yr = m.group(1).lower(), int(m.group(2))
        mon = MONTHS[mon_name]
        last_day = calendar.monthrange(yr, mon)[1]
        start = f"{yr:04d}-{mon:02d}-01"
        end = f"{yr:04d}-{mon:02d}-{last_day:02d}"
        where_parts.append(_between_condition())
        params.extend([start, end])
        inferred_bucket = inferred_bucket or "day"
        single_year = yr

    # Quarter + Year
    q = None; yr = None
    qmatch = re.search(r"\b(20\d{2})\s*q([1-4])\b", t) or re.search(r"\bq([1-4])\s*(20\d{2})\b", t)
    if qmatch:
        if qmatch.group(1).startswith("20") and qmatch.group(2):
            yr, q = int(qmatch.group(1)), int(qmatch.group(2))
        else:
            q, yr = int(qmatch.group(1)), int(qmatch.group(2))
    else:
        ordinal = re.search(r"\b(first|second|third|fourth)\s+quarter(?:\s+of|\s+in)?\s+(20\d{2})\b", t)
        if ordinal:
            qwords = {"first":1,"second":2,"third":3,"fourth":4}
            q, yr = qwords[ordinal.group(1)], int(ordinal.group(2))
    if q and yr:
        start_mon = 1 + (q - 1) * 3
        end_mon = start_mon + 2
        last_day = calendar.monthrange(yr, end_mon)[1]
        start = f"{yr:04d}-{start_mon:02d}-01"
        end = f"{yr:04d}-{end_mon:02d}-{last_day:02d}"
        where_parts.append(_between_condition())
        params.extend([start, end])
        inferred_bucket = inferred_bucket or "month"
        single_year = yr

    # Plain year
    if not any("BETWEEN" in p for p in where_parts):
        ymatch = YEAR_RE.search(t)
        if ymatch:
            yr = int(ymatch.group(1))
            where_parts.append(_year_condition())
            params.append(yr)
            inferred_bucket = inferred_bucket or "month"
            single_year = yr

    return where_parts, params, inferred_bucket, single_year


# ========= English â†’ SQL =========
def detect_orders_count(text: str) -> bool:
    t = text.lower()
    return bool(
        re.search(r"\border(s)?\s+count\b", t) or
        re.search(r"\bcount of orders\b", t) or
        re.search(r"\bnumber of orders\b", t) or
        re.search(r"\border volume\b", t)
    )

def build_join_sql(question_text, tokens, agg, where_clause):
    # Measure selection
    if detect_orders_count(question_text):
        select_measure = "COUNT(DISTINCT orders.order_id) AS value"
    else:
        is_distinct = wants_distinct(tokens)
        measure = "line_total"
        select_measure = (
            "COUNT(DISTINCT customers.customer_id) AS value"
            if is_distinct and tokens_contain(tokens, ["customer"])
            else f"{agg}({measure}) AS value"
        )

    # Detect bucket & period filters
    bucket = extract_time_bucket(tokens, question_text)
    period_parts, period_params, inferred_bucket, single_year = extract_period_filters(question_text)
    if not bucket and inferred_bucket:
        bucket = inferred_bucket

    lbl, grp, ordk, alias = period_expressions(bucket)

    # "date between â€¦ and â€¦"
    m = re.search(r"date\s+between\s+(\d{4}-\d{2}-\d{2})\s+and\s+(\d{4}-\d{2}-\d{2})", question_text.lower())
    extra_between = None
    params = []
    if m:
        dt1, dt2 = m.group(1), m.group(2)
        extra_between = _between_condition()
        params.extend([dt1, dt2])

    # WHERE
    parts = []
    if where_clause:
        parts.append(where_clause.replace("WHERE", "").strip())
    parts.extend(period_parts)
    if extra_between:
        parts.append(extra_between)
    final_where = ("WHERE " + " AND ".join(parts) + "\n") if parts else ""

    top_n = extract_top_n(tokens)
    limit_clause = f"\nLIMIT {top_n}" if top_n else ""

    # ---- Calendar fill helpers (time-only, single year) ----
    def with_calendar_fill_time_only():
        joins = base_joins()
        all_params = period_params + params
        if db_type == "SQLite":
            if bucket == "quarter":
                cal_rows = ", ".join([f"('{single_year}-Q{i}', {i})" for i in range(1,5)])
                sql = f"""
WITH cal(period, ord) AS (VALUES {cal_rows}),
agg AS (
  SELECT {lbl} AS period, {select_measure}
  {joins}{final_where}
  GROUP BY {grp}
)
SELECT cal.period, COALESCE(agg.value, 0) AS value
FROM cal
LEFT JOIN agg ON agg.period = cal.period
ORDER BY cal.ord;
""".strip()
                return sql, all_params
            if bucket == "month":
                cal_rows = ", ".join([f"('{single_year}-{m:02d}', {m})" for m in range(1,13)])
                sql = f"""
WITH cal(period, ord) AS (VALUES {cal_rows}),
agg AS (
  SELECT {lbl} AS period, {select_measure}
  {joins}{final_where}
  GROUP BY {grp}
)
SELECT cal.period, COALESCE(agg.value, 0) AS value
FROM cal
LEFT JOIN agg ON agg.period = cal.period
ORDER BY cal.ord;
""".strip()
                return sql, all_params
            if bucket == "year":
                sql = f"""
WITH cal(period, ord) AS (VALUES ('{single_year}', 1)),
agg AS (
  SELECT {lbl} AS period, {select_measure}
  {joins}{final_where}
  GROUP BY {grp}
)
SELECT cal.period, COALESCE(agg.value, 0) AS value
FROM cal
LEFT JOIN agg ON agg.period = cal.period
ORDER BY cal.ord;
""".strip()
                return sql, all_params
        else:
            # PostgreSQL
            if bucket == "quarter":
                sql = f"""
WITH cal AS (
  SELECT q AS ord, to_char(make_date({single_year}, 1 + (q-1)*3, 1), 'YYYY-"Q"Q') AS period
  FROM generate_series(1,4) AS q
),
agg AS (
  SELECT {lbl} AS period, {select_measure}
  {joins}{final_where}
  GROUP BY {grp}
)
SELECT cal.period, COALESCE(agg.value, 0) AS value
FROM cal
LEFT JOIN agg ON agg.period = cal.period
ORDER BY cal.ord;
""".strip()
                return sql, all_params
            if bucket == "month":
                sql = f"""
WITH cal AS (
  SELECT to_char(d, 'YYYY-MM') AS period, EXTRACT(MONTH FROM d)::int AS ord
  FROM generate_series(date '{single_year}-01-01', date '{single_year}-12-01', interval '1 month') AS d
),
agg AS (
  SELECT {lbl} AS period, {select_measure}
  {joins}{final_where}
  GROUP BY {grp}
)
SELECT cal.period, COALESCE(agg.value, 0) AS value
FROM cal
LEFT JOIN agg ON agg.period = cal.period
ORDER BY cal.ord;
""".strip()
                return sql, all_params
            if bucket == "year":
                sql = f"""
WITH cal(period, ord) AS (VALUES ('{single_year}', 1)),
agg AS (
  SELECT {lbl} AS period, {select_measure}
  {joins}{final_where}
  GROUP BY {grp}
)
SELECT cal.period, COALESCE(agg.value, 0) AS value
FROM cal
LEFT JOIN agg ON agg.period = cal.period
ORDER BY cal.ord;
""".strip()
                return sql, all_params
        return None, all_params

    def dim_block(dim, group_cols, include_period=False):
        joins = base_joins()
        all_params = period_params + params
        if include_period and lbl and grp:
            sql = f"SELECT {dim}, {lbl} AS {alias}, {select_measure}\n{joins}"
            if final_where: sql += final_where
            sql += f"GROUP BY {group_cols}, {grp}\n"
            sql += f"ORDER BY {group_cols}, {ordk}\n" if ordk else "ORDER BY 3 DESC\n"
            sql += f"{limit_clause};"
            return sql, all_params
        else:
            sql = f"SELECT {dim}, {select_measure}\n{joins}"
            if final_where: sql += final_where
            sql += f"GROUP BY {group_cols}\nORDER BY 2 DESC{limit_clause};"
            return sql, all_params

    # Dimensions
    if tokens_contain(tokens, ["customer", "customers"]):
        return dim_block("customers.customer_name AS dimension", "customers.customer_name", include_period=bool(lbl))
    if tokens_contain(tokens, ["product", "products"]):
        return dim_block("products.product_name AS dimension", "products.product_name", include_period=bool(lbl))
    if tokens_contain(tokens, ["category", "categories"]):
        return dim_block("products.category AS dimension", "products.category", include_period=bool(lbl))

    # Time-only
    if lbl and grp:
        if single_year and (bucket in ("month", "quarter", "year")):
            sql_fill, all_params = with_calendar_fill_time_only()
            if sql_fill:
                return sql_fill, all_params

        joins = base_joins()
        sql = f"SELECT {lbl} AS {alias}, {select_measure}\n{joins}"
        if final_where: sql += final_where
        sql += f"GROUP BY {grp}\n"
        sql += f"ORDER BY {ordk}\n" if ordk else "ORDER BY 2 DESC\n"
        sql += f"{limit_clause};"
        return (sql, period_params + params)

    # Default total
    joins = base_joins()
    sql = f"SELECT {select_measure}\n{joins}"
    if final_where: sql += final_where
    sql += "LIMIT 1;"
    return sql, period_params + params


def translate_question_to_sql(question, schema):
    tokens, _ = tokenize_and_tag(question)
    agg = next((AGG_KEYWORDS[t] for t in tokens if t in AGG_KEYWORDS), "SUM")

    where_clause = ""
    if " where " in (" " + question.lower() + " "):
        tail = question.lower().split("where", 1)[1].strip()
        m = re.match(r"([a-zA-Z_\.]+)\s*(=|>|<)\s*'?(.*?)'?$", tail)
        if m:
            col, op, val = m.groups()
            where_clause = f"WHERE {col} {op} '{val}'"

    sql, params = build_join_sql(question, tokens, agg, where_clause)
    return sql, params, None if sql else ("", [], "Couldn't translate question.")


# ========= UI: Ask + Run =========
if st.session_state.get("schema"):
    SAMPLE_QUESTIONS = [
        # time-only totals
        "total sales by month in 2023",
        "orders count by month in 2024",
        "total sales by quarter in 2025",
        "yearly revenue",

        # month / quarter specific
        "sales in January 2025",
        "orders count in January 2025",
        "sales in Q4 2025",
        "sales in Q4 2025 by category",

        # time + dimension + top-N
        "top 5 customers by total sales",
        "top 5 products by total sales in 2024",

        # explicit date between window (daily)
        "total sales by day where date between 2025-10-20 and 2025-10-25",

        # time + dimension breakdowns
        "monthly revenue in 2025 by product",
        "yearly revenue by category",
    ]

    st.subheader("ðŸ’¡ Sample Questions")
    cols = st.columns(3)
    for i, q in enumerate(SAMPLE_QUESTIONS):
        if cols[i % 3].button(q, key=f"q_{i}"):
            st.session_state["prefill"] = q

    st.subheader("ðŸ’¬ Ask a Business Question")
    default_q = st.session_state.get("prefill", SAMPLE_QUESTIONS[0])
    question = st.text_input("Type your question or pick a sample above:", value=default_q)
    run_click = st.button("Translate & Run SQL", key="run_main")

    if run_click:
        if not st.session_state.get("conn"):
            st.warning("Please connect and scan schema first.")
        else:
            sql, params, err = translate_question_to_sql(question, st.session_state["schema"])
            if err:
                st.error(err)
            else:
                st.code(sql, language="sql")
                try:
                    if db_type == "SQLite":
                        with sqlite3.connect(sqlite_path, check_same_thread=False) as _conn:
                            df = pd.read_sql_query(sql, _conn, params=params)
                    else:
                        # Postgres path (requires psycopg2 + a live connection)
                        df = pd.read_sql_query(sql, st.session_state["conn"], params=params)

                    st.subheader("ðŸ“Š Results")
                    st.dataframe(df, use_container_width=True)

                    if not df.empty:
                        st.download_button(
                            "ðŸ“¥ Download results as CSV",
                            df.to_csv(index=False).encode("utf-8"),
                            "results.csv", "text/csv", key="dl_csv_q"
                        )

                        cols_df = list(df.columns)

                        # Time series sorting for nice charts
                        if "period" in cols_df and "value" in cols_df:
                            def sort_key(val: str):
                                val = str(val)
                                m = re.match(r"^(\d{4})-(\d{2})$", val)
                                if m:
                                    yr = int(m.group(1)); mo = int(m.group(2))
                                    return (yr, mo, 0)
                                q = re.match(r"^(\d{4})-Q([1-4])$", val, re.IGNORECASE)
                                if q:
                                    yr = int(q.group(1)); qq = int(q.group(2))
                                    return (yr, qq * 3, 1)
                                y = re.match(r"^(\d{4})$", val)
                                if y:
                                    yr = int(y.group(1))
                                    return (yr, 0, 2)
                                return (9999, 99, 9)

                            df = df.sort_values(by="period", key=lambda col: col.map(sort_key))

                            if "dimension" in cols_df:
                                try:
                                    pv = df.pivot(index="period", columns="dimension", values="value")
                                    st.caption("Bar chart")
                                    st.bar_chart(pv)
                                except Exception:
                                    st.caption("Bar chart")
                                    st.bar_chart(df.set_index("period")["value"])
                            else:
                                st.caption("Bar chart")
                                st.bar_chart(df.set_index("period")["value"])

                        elif len(cols_df) >= 2 and pd.api.types.is_numeric_dtype(df.iloc[:, -1]):
                            st.caption("Bar chart")
                            st.bar_chart(df.set_index(cols_df[0]).iloc[:, -1])

                except Exception as e:
                    st.error(f"Query execution error: {e}")
else:
    # Bigger, borderless helper (with emoji)
    st.markdown(
        '<div class="helper-note">ðŸ–¥ï¸ Connect to your database on the left to begin.</div>',
        unsafe_allow_html=True
    )
    st.markdown(open("button_style.css").read(), unsafe_allow_html=True)


