# streamlit_app.py
# -*- coding: utf-8 -*-
"""
Εφαρμογή κρατήσεων (Απρίλιος–Οκτώβριος) με όμορφο UI και μόνιμη αποθήκευση σε SQLite.
- Πίνακας: X=μήνες (Απρ–Οκτ), Y=ημέρες (1–31)
- Κάθε κελί δέχεται πολλές τιμές, χωρισμένες με κόμμα, π.χ.: 22α, 23β, 24γ
  * Μορφή: YY[α/β/γ] ή προαιρετικά YY[α/β/γ]:τιμή (π.χ. 23α:120)
- Στατιστικά: πλήθος κρατήσεων ανά έτος, ανά όροφο, γραφήματα.
- Μόνιμη αποθήκευση: bookings.db στον τοπικό φάκελο.

Οδηγίες εκτέλεσης:
    streamlit run streamlit_app.py
"""

from __future__ import annotations
import re
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
import streamlit as st

APP_TITLE = "📅 Κρατήσεις Διαμερισμάτων (Απρ–Οκτ)"
DB_PATH = Path("bookings.db")

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
DAYS = list(range(1, 32))  # 1–31 (μερικοί μήνες έχουν <31 αλλά κρατάμε κοινό πλέγμα)

FLOORS = ["α", "β", "γ"]  # όροφοι
FLOOR_LABELS = {"α": "Α", "β": "Β", "γ": "Γ"}

# --- Ρυθμίσεις σελίδας & CSS αισθητικής ---
st.set_page_config(page_title=APP_TITLE, page_icon="📊", layout="wide")

CUSTOM_CSS = """
<style>
/***** Κεντρικό layout *****/
.main > div {padding-top: 0rem;}

/***** Κάρτες *****/
.card {
  background: linear-gradient(145deg, rgba(255,255,255,0.9), rgba(245,247,250,0.9));
  border: 1px solid rgba(0,0,0,0.06);
  box-shadow: 0 8px 30px rgba(0,0,0,0.06);
  border-radius: 18px;
  padding: 1.2rem 1.2rem;
}
.card h3 {
  margin: 0 0 .6rem 0;
}

/***** Πίνακας *****/
[data-testid="stDataFrame"] table {
  border-radius: 12px !important;
  overflow: hidden;
}

/***** Κουμπιά *****/
.stButton > button {
  border-radius: 999px;
  padding: .6rem 1.1rem;
  font-weight: 600;
}

/***** Κεφαλίδα *****/
h1.title {
  font-weight: 800;
  letter-spacing: -.3px;
  background: linear-gradient(90deg, #111, #666);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
}
.small-muted {color: #6b7280; font-size: .9rem}
</style>
"""

st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

st.markdown(f"<h1 class='title'>{APP_TITLE}</h1>", unsafe_allow_html=True)
st.markdown(
    "<p class='small-muted'>Συμπλήρωσε κελιά με τιμές τύπου <code>22α</code> ή <code>22α:120</code>.\n"
    "Διαχώρισε πολλαπλές εγγραφές με κόμμα· π.χ. <code>22α, 23β, 24γ</code>.</p>",
    unsafe_allow_html=True,
)

# ---------- Βοηθητικά DB ----------

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS cells (
  month TEXT NOT NULL,
  day   INTEGER NOT NULL,
  entries TEXT DEFAULT '',
  PRIMARY KEY(month, day)
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
TOKEN_RE = re.compile(r"^\s*(\d{2})([αβγ])(?:\s*:\s*(\d+(?:\.\d+)?))?\s*$", re.IGNORECASE)

# Χαρτογράφηση 2-ψηφίου έτους -> 2000+ (π.χ. 22 -> 2022)

def two_digit_to_year(two: int) -> int:
    # Υποθέτουμε 20xx. Αν θες άλλο mapping, άλλαξέ το.
    return 2000 + two


def parse_cell_entries(cell: str) -> List[Tuple[int, str, Optional[float]]]:
    """Επιστρέφει λίστα από (year, floor, price?) για ένα κελί."""
    if cell is None:
        return []
    s = str(cell).strip()
    if not s:
        return []
    out: List[Tuple[int, str, Optional[float]]] = []
    for raw in re.split(r",|;|/|\\n", s):
        token = raw.strip()
        if not token:
            continue
        m = TOKEN_RE.match(token)
        if not m:
            # Αγνόησε μη έγκυρα τμήματα αντί να σπάει όλη η αποθήκευση
            continue
        yy, floor, price = m.group(1), m.group(2).lower(), m.group(3)
        year = two_digit_to_year(int(yy))
        price_val = float(price) if price is not None else None
        out.append((year, floor, price_val))
    return out

# ---------- Φόρτωση/Αποθήκευση πλέγματος ----------

def load_grid_df() -> pd.DataFrame:
    """Φορτώνει το πλέγμα από τη βάση. Αν δεν υπάρχουν δεδομένα, δημιουργεί κενό DF."""
    with get_conn() as c:
        df = pd.read_sql_query("SELECT month, day, entries FROM cells", c)
    if df.empty:
        grid = pd.DataFrame("", index=DAYS, columns=MONTHS)
        grid.index.name = "Ημέρα"
        return grid
    # Ανακατασκευή πλέγματος
    grid = pd.DataFrame("", index=DAYS, columns=MONTHS)
    for _, row in df.iterrows():
        m = row["month"]
        d = int(row["day"])
        if m in grid.columns and d in grid.index:
            grid.at[d, m] = row["entries"] or ""
    grid.index.name = "Ημέρα"
    return grid


def save_grid_df(grid: pd.DataFrame) -> None:
    """Αποθηκεύει τον πίνακα στο cells και αναδημιουργεί τα bookings (parsed)."""
    # Αποθήκευση raw κελιών
    with get_conn() as c:
        cur = c.cursor()
        # Upsert σε κάθε κελί
        for d in grid.index:
            for m in grid.columns:
                entries = (grid.at[d, m] or "").strip()
                cur.execute(
                    "INSERT INTO cells(month, day, entries) VALUES(?,?,?)\n"
                    "ON CONFLICT(month,day) DO UPDATE SET entries=excluded.entries",
                    (m, int(d), entries),
                )
        # Ξαναχτίσε bookings (σβήσε όλα και ξαναπέρασέ τα - απλό και ασφαλές για αυτό το μέγεθος)
        cur.execute("DELETE FROM bookings")
        for d in grid.index:
            for m in grid.columns:
                entries = (grid.at[d, m] or "").strip()
                parsed = parse_cell_entries(entries)
                for (year, floor, price) in parsed:
                    cur.execute(
                        "INSERT INTO bookings(year, floor, month, day, price) VALUES(?,?,?,?,?)",
                        (year, floor, m, int(d), price),
                    )
        c.commit()

# ---------- Sidebar (λειτουργίες) ----------
with st.sidebar:
    st.header("⚙️ Ρυθμίσεις & Ενέργειες")
    st.markdown(
        "*Μορφή τιμής:* <code>YY[α/β/γ]</code> ή προαιρετικά <code>YY[α/β/γ]:τιμή</code>.",
        unsafe_allow_html=True,
    )
    st.caption("Παράδειγμα: 22α, 23α, 24β:150")

    if st.button("💾 Αποθήκευση", type="primary"):
        save_grid_df(st.session_state["grid_df"])
        st.success("Αποθηκεύτηκαν οι κρατήσεις και ενημερώθηκαν τα στατιστικά.")

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

st.markdown("""
<div class="card">
  <h3>🗂️ Πίνακας Κρατήσεων (γράψε σε κελιά: π.χ. <code>22α, 23β</code>)</h3>
</div>
""", unsafe_allow_html=True)

edited = st.data_editor(
    st.session_state["grid_df"],
    num_rows="fixed",
    use_container_width=True,
    key="booking_editor",
)
# Ενημέρωσε το state με την τρέχουσα μορφή
st.session_state["grid_df"] = edited

# ---------- Στατιστικά ----------
st.markdown("""
<div class="card">
  <h3>📈 Στατιστικά</h3>
  <div class="small-muted">Υπολογίζονται από τα αποθηκευμένα δεδομένα (πάτα «Αποθήκευση» για ενημέρωση).</div>
</div>
""", unsafe_allow_html=True)

col1, col2, col3 = st.columns([1,1,1])
with get_conn() as c:
    stats_df = pd.read_sql_query(
        "SELECT year, floor, month, day, price FROM bookings", c
    )

if stats_df.empty:
    st.info("Δεν υπάρχουν αποθηκευμένες κρατήσεις ακόμη.")
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

    # Εμφάνιση μετρικών
    with col1:
        total_all = int(per_year["κρατήσεις"].sum()) if not per_year.empty else 0
        st.metric("Σύνολο κρατήσεων (όλα τα έτη)", f"{total_all}")
    with col2:
        v2022 = int(per_year.loc[per_year["year"] == 2022, "κ