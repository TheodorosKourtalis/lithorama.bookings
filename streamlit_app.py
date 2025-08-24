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
from pandas.util import hash_pandas_object

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

with get_conn() as c:
    c.executescript(SCHEMA_SQL)

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
            with get_conn() as c:
                c.executescript(SCHEMA_SQL)
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
    st.header("⚙️ Ρυθμίσεις & Ενέργειες")
    st.markdown(
        "*Μορφή τιμής:* <code>YY</code> ή προαιρετικά <code>YY:τιμή</code> (π.χ. <code>22</code> ή <code>22:120</code>).",
        unsafe_allow_html=True,
    )
    st.caption("Παράδειγμα: 22, 22:150, 23")

    live_stats = st.toggle("Ζωντανά στατιστικά (μπορεί να καθυστερεί)", value=False, help="Αν είναι ενεργό, τα στατιστικά υπολογίζονται σε κάθε αλλαγή. Απενεργοποίησέ το για πιο ομαλή επεξεργασία.")

    if st.button("💾 Αποθήκευση", type="primary"):
        ok, err = save_grid_df(st.session_state["grid_df"])
        if ok:
            st.success("Αποθηκεύτηκαν οι κρατήσεις και ενημερώθηκαν τα στατιστικά.")
        else:
            st.error(f"Σφάλμα αποθήκευσης στη βάση: {err}")

    st.markdown("---")
    if st.button("⬇️ Εξαγωγή CSV (κρατήσεις)"):
        with get_conn() as c:
            bookings = pd.read_sql_query("SELECT year, floor, month, day, price FROM bookings", c)
        csv_bytes = bookings.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "Λήψη bookings.csv",
            data=csv_bytes,
            file_name="bookings.csv",
            mime="text/csv",
        )

# ---------- Αρχικός πίνακας (Data Editor) ----------
if "grid_df" not in st.session_state:
    st.session_state["grid_df"] = load_grid_df()

st.markdown(
    """
<div class="card">
  <h3>🗂️ Πίνακας Κρατήσεων (στήλες ανά Μήνα×Όροφο: Ισόγειο/Α/Β)</h3>
  <div class="small-muted">Γράψε σε κελιά τιμές όπως <code>22</code> ή <code>22:120</code>. Χώρισε πολλαπλές τιμές με κόμμα.</div>
</div>
""",
    unsafe_allow_html=True,
)

# Ρύθμιση text columns για ΣΥΓΚΕΚΡΙΜΕΝΗ συμπεριφορά εισαγωγής/διαγραφής
col_cfg = {col: st.column_config.TextColumn(col, help="Γράψε π.χ. 22 ή 22:120. Πολλαπλές εγγραφές με κόμμα.") for col in GRID_COLUMNS}

view_df = _norm_df(st.session_state["grid_df"])  # πάντα "" αντί για <NA>
edited = st.data_editor(
    view_df,
    num_rows="fixed",
    use_container_width=True,
    key="booking_editor",
    column_config=col_cfg,
)
# Ενημέρωση state ΜΟΝΟ αν όντως άλλαξε κάτι (αποφεύγει διπλές ενημερώσεις/lag)
current_df = _norm_df(st.session_state["grid_df"])
new_df = _norm_df(edited)
if not _frames_equal(current_df, new_df):
    st.session_state["grid_df"] = new_df

# ---------- Στατιστικά ----------
if live_stats:
    with get_conn() as c:
        stats_df = pd.read_sql_query("SELECT year, floor, month, day, price FROM bookings", c)
else:
    # Όταν τα ζωντανά στατιστικά είναι κλειστά, δείχνουμε τα τελευταία αποθηκευμένα μόνο μετά από Αποθήκευση
    try:
        with get_conn() as c:
            stats_df = pd.read_sql_query("SELECT year, floor, month, day, price FROM bookings", c)
    except Exception:
        stats_df = pd.DataFrame(columns=["year", "floor", "month", "day", "price"])  # κενό

if stats_df.empty:
    st.info("Δεν υπάρχουν αποθηκευμένες κρατήσεις ακόμη ή τα ζωντανά στατιστικά είναι απενεργοποιημένα.")
else:
    # Σύνολα ανά έτος
    per_year = stats_df.groupby("year").size().reset_index(name="κρατήσεις")
    # Σύνολα ανά έτος & όροφο
    per_year_floor = (
        stats_df.groupby(["year", "floor"]).size().reset_index(name="κρατήσεις")
    )

    # Μέσος όρος τιμής (αν έχουν εισαχθεί τιμές)
    price_info = None
    if stats_df["price"].notna().any():
        price_info = (
            stats_df.dropna(subset=["price"]).groupby("year")["price"].mean().reset_index()
        )

    col1, col2, col3 = st.columns(3)
    with col1:
        total_all = int(per_year["κρατήσεις"].sum()) if not per_year.empty else 0
        st.metric("Σύνολο κρατήσεων (όλα τα έτη)", f"{total_all}")
    with col2:
        latest_year = int(per_year["year"].max()) if not per_year.empty else None
        if latest_year:
            latest_cnt = int(per_year.loc[per_year["year"] == latest_year, "κρατήσεις"].sum())
            st.metric(f"Κρατήσεις {latest_year}", f"{latest_cnt}")
        else:
            st.metric("Κρατήσεις", "0")
    with col3:
        if price_info is not None and not price_info.empty:
            last_price_year = int(price_info["year"].max())
            mean_price = float(price_info.loc[price_info["year"] == last_price_year, "price"].iloc[0])
            st.metric(f"Μέση τιμή ({last_price_year})", f"{mean_price:.2f}")
        else:
            st.metric("Μέση τιμή", "—")

    st.subheader("Ανά έτος")
    st.dataframe(per_year, use_container_width=True)

    st.subheader("Ανά έτος & όροφο")
    st.dataframe(per_year_floor, use_container_width=True)

    if price_info is not None and not price_info.empty:
        st.subheader("Μέση τιμή ανά έτος")
        st.dataframe(price_info.rename(columns={"price": "μέση_τιμή"}), use_container_width=True)
