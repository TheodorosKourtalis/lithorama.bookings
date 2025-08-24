# streamlit_app.py
# -*- coding: utf-8 -*-
"""
Εφαρμογή κρατήσεων (Απρίλιος–Οκτώβριος) με όμορφο UI και μόνιμη αποθήκευση σε SQLite.
- ΔΟΜΗ ΠΙΝΑΚΑ: Για ΚΑΘΕ μήνα υπάρχουν 3 ξεχωριστές στήλες (Ισόγειο, Α, Β).
  Παράδειγμα στηλών: «Απρίλιος Ισόγειο», «Απρίλιος Α», «Απρίλιος Β», «Μάιος Ισόγειο», ...
- Κάθε κελί δέχεται πολλές τιμές (μία ή περισσότερες κρατήσεις) χωρισμένες με κόμμα.
  * Μορφή εγγραφής: YY ή προαιρετικά YY:τιμή (π.χ. 22 ή 22:120)
  * ΔΕΝ γράφουμε πια α/β/γ μέσα στο κελί — ο όροφος προκύπτει από τη στήλη.
- Στατιστικά: πλήθος κρατήσεων ανά έτος, ανά όροφο, και μέση τιμή.
- Μόνιμη αποθήκευση: bookings.db στον τοπικό φάκελο.

Οδηγίες εκτέλεσης:
    streamlit run streamlit_app.py
"""

from __future__ import annotations
import re
import os
import sqlite3
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
import streamlit as st

APP_TITLE = "📅 Κρατήσεις Διαμερισμάτων (Απρ–Οκτ)"
# Χρησιμοποιούμε επίμονο φάκελο στο Streamlit Cloud (/mount/data) αν υπάρχει/είναι εγγράψιμος
_DATA_DIR = Path("/mount/data")
if _DATA_DIR.exists() and os.access(_DATA_DIR, os.W_OK):
    DB_PATH = _DATA_DIR / "bookings.db"
else:
    DB_PATH = Path("bookings.db")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# Μήνες (Απρίλιος–Οκτώβριος)
MONTHS = [
    "Απρίλιος",
    "Μάιος",
    "Ιούνιος",
    "Ιούλιος",
    "Αύγουστος",
    "Σεπτέμβριος",
    "Οκτώβριος",
]
DAYS = list(range(1, 32))  # 1–31

# Όροφοι (εμφάνιση)
FLOORS_DISPLAY = ["Ισόγειο", "Α", "Β"]

# Χαρτογράφηση εμφάνισης -> τι θα γράφεται στη ΒΔ
FLOOR_DB_VALUE = {
    "Ισόγειο": "Ισόγειο",
    "Α": "Α",
    "Β": "Β",
}

# Παράγουμε τις στήλες του πλέγματος ως (Μήνας + space + Όροφος)
GRID_COLUMNS = [f"{m} {f}" for m in MONTHS for f in FLOORS_DISPLAY]

# --- Ρυθμίσεις σελίδας & CSS αισθητικής ---
st.set_page_config(page_title=APP_TITLE, page_icon="📊", layout="wide")

CUSTOM_CSS = """
<style>
/***** Κεντρικό layout *****/
.main > div {padding-top: 0rem;}

/***** Κάρτες *****/
.card {
  background: #ffffff; /* ουδέτερο λευκό */
  border: 1px solid rgba(0,0,0,0.08);
  box-shadow: 0 2px 12px rgba(0,0,0,0.04);
  border-radius: 12px;
  padding: 1rem 1rem;
}
.card h3 { margin: 0 0 .6rem 0; color: inherit; }

/***** Πίνακας *****/
[data-testid="stDataFrame"] table { border-radius: 8px !important; overflow: hidden; }

/***** Κουμπιά *****/
.stButton > button { border-radius: 10px; padding: .5rem .9rem; font-weight: 600; }

/***** Κεφαλίδα *****/
h1.title {
  font-weight: 800; letter-spacing: -.2px; color: inherit; /* χωρίς gradients */
}
.small-muted {color: #6b7280; font-size: .9rem}
</style>
"""

st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

st.markdown(f"<h1 class='title'>{APP_TITLE}</h1>", unsafe_allow_html=True)
st.markdown(
    "<p class='small-muted'>Γράψε σε κάθε κελί πολλά στοιχεία χωρισμένα με κόμμα, π.χ. <code>22</code> ή <code>22:120</code>.\n"
    "Δεν βάζουμε α/β/γ — αυτό βγαίνει από τη στήλη (Ισόγειο/Α/Β).</p>",
    unsafe_allow_html=True,
)

# ---------- Βάση δεδομένων ----------

def get_conn() -> sqlite3.Connection:
    # check_same_thread False για να μην σκάει σε reruns/πολλαπλά threads του Streamlit
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

# Χρησιμοποιούμε νέο σχήμα για να αποφύγουμε συγκρούσεις με παλιά tables
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS cells (
  month  TEXT    NOT NULL,
  floor  TEXT    NOT NULL,
  day    INTEGER NOT NULL,
  entries TEXT DEFAULT '',
  PRIMARY KEY(month, floor, day)
);

CREATE TABLE IF NOT EXISTS bookings (
  id      INTEGER PRIMARY KEY AUTOINCREMENT,
  year    INTEGER NOT NULL,
  floor   TEXT    NOT NULL,
  month   TEXT    NOT NULL,
  day     INTEGER NOT NULL,
  price   REAL
);

CREATE INDEX IF NOT EXISTS idx_bookings_year ON bookings(year);
CREATE INDEX IF NOT EXISTS idx_bookings_floor ON bookings(floor);
CREATE INDEX IF NOT EXISTS idx_bookings_month ON bookings(month);
"""

def ensure_schema() -> None:
    with get_conn() as c:
        # Αν δεν υπάρχει καθόλου, φτιάξ' το
        c.executescript(SCHEMA_SQL)
        try:
            cols = pd.read_sql_query("PRAGMA table_info(cells)", c)
            colnames = set(cols["name"].tolist())
            if "floor" not in colnames:
                # Μεταφορά από παλιό σχήμα: month, day, entries
                c.execute("BEGIN")
                c.execute("ALTER TABLE cells RENAME TO cells_old;")
                c.executescript(SCHEMA_SQL)
                # Βάλε τα παλιά ως Ισόγειο
                c.execute(
                    "INSERT INTO cells(month, floor, day, entries)\n"
                    "SELECT month, 'Ισόγειο' AS floor, day, entries FROM cells_old;"
                )
                c.execute("DROP TABLE cells_old;")
                c.execute("COMMIT")
        except Exception:
            # Αν κάτι πάει στραβά, ας μην μπλοκάρουμε την εφαρμογή
            pass

ensure_schema()

# ---------- Parsing κρατήσεων ----------
# Δεχόμαστε: 2-ψηφιο έτος και προαιρετική τιμή: 22 ή 22:120
TOKEN_RE = re.compile(r"^\s*(\d{2})(?:\s*:\s*(\d+(?:\.\d+)?))?\s*$")


def two_digit_to_year(two: int) -> int:
    return 2000 + two


def parse_cell_entries(cell: str) -> List[Tuple[int, Optional[float]]]:
    """Επιστρέφει λίστα από (year, price?) για ένα κελί (όροφος προκύπτει από τη στήλη)."""
    if cell is None:
        return []
    s = str(cell).strip()
    if not s:
        return []
    out: List[Tuple[int, Optional[float]]] = []
    for raw in re.split(r",|;|/|\\n", s):
        token = raw.strip()
        if not token:
            continue
        m = TOKEN_RE.match(token)
        if not m:
            # Αγνόησε μη έγκυρα τμήματα
            continue
        yy, price = m.group(1), m.group(2)
        year = two_digit_to_year(int(yy))
        price_val = float(price) if price is not None else None
        out.append((year, price_val))
    return out

# ---------- Βοηθητικά για στήλες ----------

def split_month_floor(col: str) -> Tuple[str, str]:
    """Δέχεται στήλη τύπου 'Απρίλιος Α' και επιστρέφει (month, floor-display)."""
    # Δεδομένου ότι τα floor tokens δεν περιέχουν κενά, το split από τα δεξιά είναι ασφαλές
    month, floor = col.rsplit(" ", 1)
    return month, floor

# ---------- Φόρτωση/Αποθήκευση πλέγματος ----------

def empty_grid() -> pd.DataFrame:
    grid = pd.DataFrame("", index=DAYS, columns=GRID_COLUMNS, dtype="string")
    grid.index.name = "Ημέρα"
    return grid


def load_grid_df() -> pd.DataFrame:
    try:
        with get_conn() as c:
            df = pd.read_sql_query("SELECT month, floor, day, entries FROM cells", c)
    except Exception:
        # Αν για οποιονδήποτε λόγο λείπει ο πίνακας ή υπάρχει παλιά έκδοση, ξαναφτιάξ’ τον και δώσε κενό πλέγμα
        try:
            ensure_schema()
        except Exception:
            pass
        return empty_grid()
    if df.empty:
        return empty_grid()
    grid = empty_grid()
    for _, row in df.iterrows():
        col = f"{row['month']} {row['floor']}"
        d = int(row["day"])
        if col in grid.columns and d in grid.index:
            grid.at[d, col] = row["entries"] or ""
    # Εξαναγκάζουμε string dtype ώστε να μην εμφανίζονται NaN/παλιές τιμές
    grid = grid.astype("string")
    return grid


def _norm_df(df: pd.DataFrame) -> pd.DataFrame:
    # Εξαναγκάζουμε ίδια διάταξη/τύπους και άδειο string αντί για <NA>
    df2 = df.reindex(index=DAYS, columns=GRID_COLUMNS)
    df2 = df2.astype("string").fillna("")
    return df2

def save_grid_df(grid: pd.DataFrame) -> Tuple[bool, Optional[str]]:
    grid = _norm_df(grid)
    try:
        with get_conn() as c:
            cur = c.cursor()
            # Upsert όλων των κελιών
            for d in grid.index:
                for col in grid.columns:
                    val = grid.at[d, col]
                    entries = "" if pd.isna(val) else str(val).strip()
                    month, floor_disp = split_month_floor(col)
                    floor_db = FLOOR_DB_VALUE[floor_disp]
                    cur.execute(
                        "INSERT INTO cells(month, floor, day, entries) VALUES(?,?,?,?)\n"
                        "ON CONFLICT(month,floor,day) DO UPDATE SET entries=excluded.entries",
                        (month, floor_db, int(d), entries),
                    )
            # Αναδημιουργία bookings (σβήσε όλα και ξαναπέρασε τα parsed)
            cur.execute("DELETE FROM bookings")
            for d in grid.index:
                for col in grid.columns:
                    val = grid.at[d, col]
                    entries = "" if pd.isna(val) else str(val).strip()
                    month, floor_disp = split_month_floor(col)
                    floor_db = FLOOR_DB_VALUE[floor_disp]
                    parsed = parse_cell_entries(entries)
                    for (year, price) in parsed:
                        cur.execute(
                            "INSERT INTO bookings(year, floor, month, day, price) VALUES(?,?,?,?,?)",
                            (year, floor_db, month, int(d), price),
                        )
            c.commit()
        return True, None
    except Exception as e:
        return False, str(e)

def _frames_equal(a: pd.DataFrame, b: pd.DataFrame) -> bool:
    a2 = _norm_df(a)
    b2 = _norm_df(b)
    return a2.equals(b2)

# ---------- Sidebar (λειτουργίες) ----------
with st.sidebar:
    st.header("ℹ️ Οδηγίες")
    st.markdown(
        "Γράψε σε κελιά τιμές όπως **22** ή **22:120**. Χώρισε πολλαπλές τιμές με κόμμα.\n\n"
        "Οι αλλαγές **δεν** αποθηκεύονται αυτόματα — πατάς **Αποθήκευση** στο κάτω μέρος του πίνακα.")
    st.markdown("—")
    # Προαιρετικό: export κουμπί θα το βάλουμε κάτω, μετά την Αποθήκευση.

# ---------- Πίνακας (HTML‑styled) με φόρμα αποθήκευσης ----------
if "grid_df" not in st.session_state:
    st.session_state["grid_df"] = load_grid_df()

st.markdown(
    """
<div class="card">
  <h3>🗂️ Πίνακας Κρατήσεων (Μήνας × Όροφος: Ισόγειο/Α/Β)</h3>
  <div class="small-muted">Οι αλλαγές καταχωρούνται όταν πατήσεις <strong>Αποθήκευση</strong> στο τέλος.</div>
</div>
""",
    unsafe_allow_html=True,
)

# Βοηθητική για labels
def _label(month: str, floor: str, day: int) -> str:
    return f"{month} {floor} — {day}"

with st.form("booking_form", clear_on_submit=False):
    tabs = st.tabs(MONTHS)
    # Θα συλλέξουμε τις τιμές εδώ
    new_values = {}
    for i, m in enumerate(MONTHS):
        with tabs[i]:
            st.markdown(f"### {m}")
            # HTML‑styled table header
            st.markdown(
                "<div style='display:grid;grid-template-columns:80px 1fr 1fr 1fr;gap:6px;font-weight:600;'>"
                "<div>Ημέρα</div><div>Ισόγειο</div><div>Α</div><div>Β</div>"
                "</div>",
                unsafe_allow_html=True,
            )
            for d in DAYS:
                cols = st.columns([0.7, 1, 1, 1], gap="small")
                cols[0].markdown(f"**{d}**")
                for j, f in enumerate(FLOORS_DISPLAY, start=1):
                    colname = f"{m} {f}"
                    initial = st.session_state["grid_df"].at[d, colname] if (d in st.session_state["grid_df"].index and colname in st.session_state["grid_df"].columns) else ""
                    key = f"cell::{m}::{f}::{d}"
                    val = cols[j].text_input(_label(m, f, d), value=str(initial or ""), key=key, label_visibility="collapsed")
                    new_values[(d, colname)] = val
    submitted = st.form_submit_button("💾 Αποθήκευση", type="primary")

# Αν πατήθηκε Αποθήκευση, αναδομούμε DataFrame και γράφουμε στη ΒΔ
if submitted:
    updated = st.session_state["grid_df"].copy()
    for (d, colname), v in new_values.items():
        if colname in updated.columns and d in updated.index:
            updated.at[d, colname] = str(v or "")
    st.session_state["grid_df"] = updated.astype("string").fillna("")
    ok, err = save_grid_df(st.session_state["grid_df"])
    if ok:
        st.success("Αποθηκεύτηκαν οι κρατήσεις.")
    else:
        st.error(f"Σφάλμα αποθήκευσης: {err}")

    # Προσφέρουμε export μετά την επιτυχή αποθήκευση
    if ok:
        with get_conn() as c:
            bookings = pd.read_sql_query("SELECT year, floor, month, day, price FROM bookings", c)
        csv_bytes = bookings.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "⬇️ Λήψη bookings.csv",
            data=csv_bytes,
            file_name="bookings.csv",
            mime="text/csv",
        )
