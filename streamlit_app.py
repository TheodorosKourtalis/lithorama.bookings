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
.main > div { padding-top: 0rem; }

/***** Κάρτες (χωρίς χρώματα/σκιές) *****/
.card { border-radius: 12px; padding: 1rem 1rem; }
.card h3 { margin: 0 0 .6rem 0; }

/***** Κουμπιά *****/
.stButton > button { border-radius: 10px; padding: .5rem .9rem; font-weight: 600; }

/***** Κεφαλίδα (χωρίς χρώμα) *****/
h1.title { font-weight: 800; letter-spacing: -.2px; }
.small-muted { font-size: .9rem }

/* Headers ευθυγραμμισμένα και responsive */
.col-header { text-align: center; font-weight: 600; }
.day-cell { text-align: center; font-weight: 600; }
@media (max-width: 768px) { .small-muted { font-size: .95rem; } }
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

# --- Serialize entries helper ---
def serialize_entries(entries: List[Tuple[int, Optional[float]]]) -> str:
    """Δέχεται λίστα (year, price?) και επιστρέφει tokens τύπου 'YY' ή 'YY:price' χωρισμένα με κόμμα."""
    toks = []
    for (y, p) in entries:
        yy = int(y) % 100
        if p is None:
            toks.append(f"{yy:02d}")
        else:
            toks.append(f"{yy:02d}:{float(p):g}")
    return ",".join(toks)

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
    # CSV upload + merge/replace logic
    st.subheader("Εισαγωγή από CSV")
    up = st.file_uploader("Επίλεξε CSV", type=["csv"], help="Δέχεται είτε long-format bookings.csv (year,floor,month,day,price) είτε grid-format με στήλες μήνας+όροφος και προαιρετική στήλη Ημέρα.")
    merge_mode = st.radio(
        "Τρόπος ενημέρωσης",
        ["Αντικατάσταση όλων", "Συγχώνευση (μόνο μη κενά)"],
        index=1,
        help="Αντικατάσταση: το αρχείο αντικαθιστά όλο το πλέγμα. Συγχώνευση: μόνο τα μη κενά του αρχείου γράφουν πάνω στα υπάρχοντα.",
    )
    if up is not None and st.button("↪︎ Ενημέρωση πίνακα από CSV"):
        try:
            src = pd.read_csv(up)
            # Δοκίμασε long-format bookings
            required_long = {"year", "floor", "month", "day"}
            if required_long.issubset(set(map(str.lower, src.columns))):
                # Κανονικοποίηση ονομάτων
                cols_map = {c: c.lower() for c in src.columns}
                df = src.rename(columns=cols_map)
                # Φτιάξε κενό grid
                new_grid = empty_grid()
                # Για κάθε εγγραφή σχημάτισε token YY ή YY:price και πρόσθεσέ το στο αντίστοιχο κελί
                for _, r in df.iterrows():
                    try:
                        y = int(r["year"]) % 100
                        m = str(r["month"]).strip()
                        f = str(r["floor"]).strip()
                        d = int(r["day"]) if not pd.isna(r["day"]) else None
                        if m not in MONTHS or f not in FLOORS_DISPLAY or d not in DAYS:
                            continue
                        token = f"{y:02d}"
                        if "price" in df.columns and not pd.isna(r.get("price")):
                            token = f"{token}:{float(r['price']):g}"
                        col = f"{m} {f}"
                        prev = str(new_grid.at[d, col] or "").strip()
                        new_grid.at[d, col] = (prev + ("," if prev and token else "") + token) if token else prev
                    except Exception:
                        continue
            else:
                # Προσπάθησε grid-format: μπορεί να έχει στήλη Ημέρα
                df = src.copy()
                if "Ημέρα" in df.columns:
                    df = df.set_index("Ημέρα")
                # Περιορίζουμε σε γνωστές στήλες
                keep_cols = [c for c in df.columns if c in GRID_COLUMNS]
                new_grid = empty_grid()
                if keep_cols:
                    new_grid.loc[new_grid.index, keep_cols] = df[keep_cols].astype("string").reindex(index=DAYS).fillna("")
                else:
                    st.error("Το CSV δεν αναγνωρίστηκε (ούτε bookings long-format ούτε grid-format με σωστές στήλες).")
                    new_grid = None

            if new_grid is not None:
                base = st.session_state.get("grid_df", empty_grid())
                if merge_mode.startswith("Αντικατάσταση"):
                    st.session_state["grid_df"] = _norm_df(new_grid)
                else:
                    # Συγχώνευση: κρατάμε τα παλιά εκτός αν το νέο έχει μη κενό
                    merged = base.copy().astype("string")
                    for col in GRID_COLUMNS:
                        left = merged[col].fillna("")
                        right = new_grid[col].fillna("")
                        merged[col] = np.where(right.astype(str).str.strip() != "", right, left)
                    st.session_state["grid_df"] = _norm_df(merged)
                st.success("Ο πίνακας ενημερώθηκε από το CSV. Μην ξεχάσεις να πατήσεις Αποθήκευση αν θες να γραφτεί στη βάση.")
        except Exception as e:
            st.error(f"Αποτυχία ανάγνωσης CSV: {e}")

    st.markdown("—")
    st.subheader("Καθαρισμός")
    month_to_clear = st.selectbox("Μήνας για καθάρισμα", MONTHS)
    if st.button("🧹 Καθάρισε τον μήνα (όλα τα έτη)"):
        base = st.session_state.get("grid_df", empty_grid()).copy()
        for f in FLOORS_DISPLAY:
            col = f"{month_to_clear} {f}"
            if col in base.columns:
                base.loc[:, col] = ""
        st.session_state["grid_df"] = _norm_df(base)
        st.success(f"Καθαρίστηκε ο {month_to_clear}. Πάτα Αποθήκευση για να γραφτεί στη βάση.")
    if st.button("🧨 Καθάρισε ΟΛΟΥΣ τους μήνες (όλα τα έτη)"):
        st.session_state["grid_df"] = empty_grid()
        st.warning("Καθαρίστηκαν όλοι οι μήνες. Πάτα Αποθήκευση για να γραφτεί στη βάση.")

    st.markdown("—")
    st.subheader("Καθαρισμός μήνα για συγκεκριμένο έτος")
    clear_year = st.number_input("Έτος", min_value=2000, max_value=2100, value=pd.Timestamp.today().year, step=1, key="clear_year_input")
    clear_month = st.selectbox("Μήνας", MONTHS, key="clear_month_select")
    if st.button("🧽 Καθάρισε τον μήνα για το έτος", key="btn_clear_month_year"):
        base = st.session_state.get("grid_df", empty_grid()).copy().astype("string").fillna("")
        yy_target = int(clear_year)
        # Για κάθε όροφο και μέρα στον επιλεγμένο μήνα, αφαίρεσε μόνο τα tokens του συγκεκριμένου έτους
        for f in FLOORS_DISPLAY:
            col = f"{clear_month} {f}"
            if col not in base.columns:
                continue
            for d in base.index:
                cell_text = str(base.at[d, col] or "").strip()
                if not cell_text:
                    continue
                tokens = parse_cell_entries(cell_text)
                kept = [(y, p) for (y, p) in tokens if int(y) != yy_target]
                base.at[d, col] = serialize_entries(kept)
        st.session_state["grid_df"] = _norm_df(base)
        st.success(f"Καθαρίστηκαν τα δεδομένα του {clear_month} για το έτος {clear_year}. Πάτα Αποθήκευση για να γραφτούν στη βάση.")

# ---------- Πίνακας (HTML‑styled) με φόρμα αποθήκευσης ----------
main_tabs = st.tabs(["Καταχώρηση", "Στατιστικά"])  # δύο σελίδες: εισαγωγή & στατιστικά

with main_tabs[0]:
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

    # Επιλογή Έτους για καταχώρηση (ώστε ο χρήστης να γράφει μόνο τιμές π.χ. 100)
    current_year = st.number_input("Έτος καταχώρησης", min_value=2000, max_value=2100, value=pd.Timestamp.today().year, step=1)
    yy_current = int(current_year) % 100
    st.caption("Αν γράψεις μόνο αριθμούς (π.χ. 100), θα θεωρηθεί τιμή για το επιλεγμένο έτος.")

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
                # Headers aligned with the same column layout (mobile friendly)
                header_cols = st.columns([0.7, 1, 1, 1], gap="small")
                header_cols[0].markdown("<div class='col-header'>Ημέρα</div>", unsafe_allow_html=True)
                header_cols[1].markdown("<div class='col-header'>Ισόγειο</div>", unsafe_allow_html=True)
                header_cols[2].markdown("<div class='col-header'>Α</div>", unsafe_allow_html=True)
                header_cols[3].markdown("<div class='col-header'>Β</div>", unsafe_allow_html=True)

                for d in DAYS:
                    cols = st.columns([0.7, 1, 1, 1], gap="small")
                    cols[0].markdown(f"<div class='day-cell'>{d}</div>", unsafe_allow_html=True)
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
            if colname not in updated.columns or d not in updated.index:
                continue
            new_text = str(v or "").strip()
            # Παλιό περιεχόμενο κελιάς
            old_text = str(updated.at[d, colname] or "").strip()
            old_parsed = parse_cell_entries(old_text)
            # Κρατάμε tokens από παλιά που ΔΕΝ ανήκουν στο τρέχον έτος
            keep_old = [(y, p) for (y, p) in old_parsed if (int(y) % 100) != yy_current]
            # Νέα tokens από input: μπορεί να είναι με έτος (YY ή YY:price) ή μόνο τιμές
            entered = [t.strip() for t in re.split(r",|;|/|\n", new_text) if t.strip()]
            new_year_tokens: List[Tuple[int, Optional[float]]] = []
            for tok in entered:
                m = TOKEN_RE.match(tok)
                if m:
                    yy, price = m.group(1), m.group(2)
                    y_full = 2000 + int(yy)
                    price_val = float(price) if price is not None else None
                    new_year_tokens.append((y_full, price_val))
                else:
                    # Μόνο τιμή → δέσε την στο επιλεγμένο έτος
                    if re.fullmatch(r"\d+(?:\.\d+)?", tok):
                        new_year_tokens.append((int(current_year), float(tok)))
                    else:
                        # αγνόησε μη έγκυρο token
                        pass
            # Αν ο χρήστης άφησε κενό, σημαίνει διαγραφή των εγγραφών του τρέχοντος έτους
            merged_tokens = keep_old + new_year_tokens
            updated.at[d, colname] = serialize_entries(merged_tokens)
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

# ---------- Στατιστικά (δεύτερη σελίδα) ----------
with main_tabs[1]:
    st.markdown(
        """
    <div class="card">
      <h3>📈 Στατιστικά Κρατήσεων</h3>
      <div class="small-muted">Τα στατιστικά βασίζονται στα δεδομένα που έχουν αποθηκευτεί στη βάση.</div>
    </div>
    """,
        unsafe_allow_html=True,
    )
    try:
        with get_conn() as c:
            stats_df = pd.read_sql_query("SELECT year, floor, month, day, price FROM bookings", c)
    except Exception as e:
        stats_df = pd.DataFrame(columns=["year", "floor", "month", "day", "price"])  # κενό/ασφαλές
        st.error(f"Δεν ήταν δυνατή η ανάγνωση στατιστικών: {e}")

    if stats_df.empty:
        st.info("Δεν υπάρχουν ακόμη αποθηκευμένες κρατήσεις.")
    else:
        # ---- Φίλτρα ----
        floors_sel = st.multiselect("Όροφοι", FLOORS_DISPLAY, default=FLOORS_DISPLAY)
        years_available = (
            sorted(stats_df["year"].dropna().astype(int).unique().tolist())
            if not stats_df.empty else []
        )
        if years_available:
            y_min, y_max = years_available[0], years_available[-1]
            if y_min == y_max:
                year_range = (y_min, y_max)
                st.caption(f"Διαθέσιμο μόνο έτος: {y_min}")
            else:
                year_range = st.slider("Έτη", min_value=y_min, max_value=y_max, value=(y_min, y_max))
        else:
            y_min, y_max = 0, 0
            year_range = (y_min, y_max)

        # Καθαρισμός/ταξινόμηση μηνών
        stats_df["month"] = pd.Categorical(stats_df["month"], categories=MONTHS, ordered=True)
        # Εφαρμογή φίλτρων
        fdf = stats_df[stats_df["floor"].isin(floors_sel)]
        fdf = fdf[(fdf["year"] >= year_range[0]) & (fdf["year"] <= year_range[1])]

        # ---- KPIs ----
        per_year = fdf.groupby("year").size().reset_index(name="κρατήσεις")
        per_year_floor = fdf.groupby(["year", "floor"]).size().reset_index(name="κρατήσεις")
        total_all = int(per_year["κρατήσεις"].sum()) if not per_year.empty else 0
        latest_year = int(per_year["year"].max()) if not per_year.empty else None

        if fdf["price"].notna().any():
            price_mean = (
                fdf.dropna(subset=["price"]).groupby("year")["price"].mean().reset_index()
                .rename(columns={"price": "μέση_τιμή"})
            )
            revenue = fdf.dropna(subset=["price"]).groupby("year")["price"].sum().reset_index().rename(columns={"price": "έσοδα"})
        else:
            price_mean = pd.DataFrame(columns=["year", "μέση_τιμή"])  # κενό
            revenue = pd.DataFrame(columns=["year", "έσοδα"])  # κενό

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Σύνολο κρατήσεων", f"{total_all}")
        with col2:
            st.metric("Τελευταίο έτος", f"{latest_year}" if latest_year else "—")
        with col3:
            if not price_mean.empty:
                last_y = int(price_mean["year"].max())
                mean_p = float(price_mean.loc[price_mean["year"] == last_y, "μέση_τιμή"].iloc[0])
                st.metric(f"Μέση τιμή ({last_y})", f"{mean_p:.2f}")
            else:
                st.metric("Μέση τιμή", "—")
        with col4:
            if not revenue.empty:
                last_y = int(revenue["year"].max())
                rev = float(revenue.loc[revenue["year"] == last_y, "έσοδα"].iloc[0])
                st.metric(f"Έσοδα ({last_y})", f"{rev:.0f}")
            else:
                st.metric("Έσοδα", "—")

        # ---- Διαγράμματα ----
        st.subheader("Κρατήσεις ανά έτος")
        if not per_year.empty:
            st.bar_chart(per_year.set_index("year"))
        else:
            st.info("Δεν υπάρχουν δεδομένα για το εύρος ετών/ορόφων που επέλεξες.")

        st.subheader("Κρατήσεις ανά έτος & όροφο")
        if not per_year_floor.empty:
            # Pivot για stacked visualization
            pv = per_year_floor.pivot(index="year", columns="floor", values="κρατήσεις").fillna(0)
            st.bar_chart(pv)
        else:
            st.info("Δεν υπάρχουν δεδομένα για τους ορόφους που επέλεξες.")

        # Μηνιαία εποχικότητα (σωρευτικά)
        st.subheader("Κρατήσεις ανά μήνα (εποχικότητα)")
        per_month = fdf.groupby("month").size().reindex(MONTHS).fillna(0)
        st.line_chart(per_month)

        # Θερμικός χάρτης: Ημέρα × Μήνας (πλήθος κρατήσεων)
        st.subheader("Heatmap: Ημέρα × Μήνας")
        hm = fdf.groupby(["month", "day"]).size().reset_index(name="count")
        # Εξασφάλιση σειράς μηνών
        hm["month"] = pd.Categorical(hm["month"], categories=MONTHS, ordered=True)
        st.vega_lite_chart(
            hm,
            {
                "mark": "rect",
                "encoding": {
                    "x": {"field": "day", "type": "ordinal", "title": "Ημέρα"},
                    "y": {"field": "month", "type": "ordinal", "sort": MONTHS, "title": "Μήνας"},
                    "color": {"field": "count", "type": "quantitative", "title": "Κρατήσεις"},
                    "tooltip": [
                        {"field": "month", "type": "ordinal", "title": "Μήνας"},
                        {"field": "day", "type": "ordinal", "title": "Ημέρα"},
                        {"field": "count", "type": "quantitative", "title": "Κρατήσεις"}
                    ]
                },
                "width": "container",
                "height": 280
            },
            use_container_width=True,
        )

        # Κατανομή τιμών (αν υπάρχουν)
        if fdf["price"].notna().any():
            st.subheader("Κατανομή Τιμών")
            prices_df = fdf[["price"]].dropna()
            st.vega_lite_chart(
                prices_df,
                {
                    "mark": "bar",
                    "encoding": {
                        "x": {"field": "price", "type": "quantitative", "bin": {"maxbins": 20}, "title": "Τιμή"},
                        "y": {"aggregate": "count", "type": "quantitative", "title": "Συχνότητα"}
                    },
                    "width": "container",
                    "height": 240
                },
                use_container_width=True,
            )

            st.subheader("Μέση τιμή ανά έτος")
            st.line_chart(price_mean.set_index("year"))

            st.subheader("Έσοδα ανά έτος")
            st.bar_chart(revenue.set_index("year"))

        # Πίνακες δεδομένων
        with st.expander("Πίνακες δεδομένων"):
            st.write("**Ανά έτος**")
            st.dataframe(per_year, use_container_width=True)
            st.write("**Ανά έτος & όροφο**")
            st.dataframe(per_year_floor, use_container_width=True)
            if not price_mean.empty:
                st.write("**Μέση τιμή ανά έτος**")
                st.dataframe(price_mean, use_container_width=True)
            if not revenue.empty:
                st.write("**Έσοδα ανά έτος**")
                st.dataframe(revenue, use_container_width=True)
