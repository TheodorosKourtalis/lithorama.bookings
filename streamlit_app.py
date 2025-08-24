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
try:
    import openpyxl  # for reading .xlsx
    HAS_OPENPYXL = True
except Exception:
    HAS_OPENPYXL = False

try:
    import xlsxwriter  # for writing .xlsx
    HAS_XLSXWRITER = True
except Exception:
    HAS_XLSXWRITER = False
APP_TITLE = "📅 Κρατήσεις Διαμερισμάτων (Απρ–Οκτ)"
# Χρησιμοποιούμε επίμονο φάκελο στο Streamlit Cloud (/mount/data) αν υπάρχει/είναι εγγράψιμος
_DATA_DIR = Path("/mount/data")
if _DATA_DIR.exists() and os.access(_DATA_DIR, os.W_OK):
    DB_PATH = _DATA_DIR / "bookings.db"
else:
    DB_PATH = Path("bookings.db")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

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

# Μήνες EN για αρχεία (όνομα αρχείου)
MONTH_EN = {
    "Απρίλιος": "APRIL",
    "Μάιος": "MAY",
    "Ιούνιος": "JUNE",
    "Ιούλιος": "JULY",
    "Αύγουστος": "AUGUST",
    "Σεπτέμβριος": "SEPTEMBER",
    "Οκτώβριος": "OCTOBER",
}

# Όροφοι (εμφάνιση)
FLOORS_DISPLAY = ["Ισόγειο", "Α", "Β"]

# Χαρτογράφηση εμφάνισης -> τι θα γράφεται στη ΒΔ
FLOOR_DB_VALUE = {
    "Ισόγειο": "Ισόγειο",
    "Α": "Α",
    "Β": "Β",
}

GRID_COLUMNS = [f"{m} {f}" for m in MONTHS for f in FLOORS_DISPLAY]

# -------- Per-year & per‑month file layout --------
# One Excel per (year, month): dev_{YYYY}_{MONTHEN}.xlsx

DATA_DIR = Path(".")
BOOKINGS_XLSX = DATA_DIR / "bookings.xlsx"
TOKEN_DEV_RE = re.compile(r"^(\d+(?:\.\d+)?):(\d{4});([A-Z]+)$")

def month_en_of(month_gr: str) -> str:
    return MONTH_EN[month_gr]

def dev_path_for(year: int, month_en: str) -> Path:
    return DATA_DIR / f"dev_{int(year)}_{month_en.upper()}.xlsx"

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
    "<p class='small-muted'>Πληκτρολόγησε μόνο την <code>τιμή</code>. Η εφαρμογή τη γράφει ως token <code>τιμή:YYYY;MONTH</code> (π.χ. <code>80:2023;AUGUST</code>). Κάθε κελί δέχεται πολλά tokens χωρισμένα με κόμμα, αλλά το κλειδί <code>YYYY;MONTH</code> είναι μοναδικό: η νέα τιμή αντικαθιστά την παλιά για το ίδιο έτος/μήνα.</p>",
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

# --- Helpers: Επιλογή ενός token και εμφάνιση τιμής για το τρέχον έτος ---
def select_single_token(tokens: List[Tuple[int, Optional[float]]], current_year: int) -> Optional[Tuple[int, Optional[float]]]:
    """Επέλεξε ένα token: προτεραιότητα στο τρέχον έτος, αλλιώς της νεότερης χρονιάς."""
    if not tokens:
        return None
    cur = [t for t in tokens if int(t[0]) == int(current_year)]
    if cur:
        return cur[-1]
    return max(tokens, key=lambda t: int(t[0]))

def display_price_for_year(cell_text: str, current_year: int) -> str:
    """Εμφάνισε μόνο την τιμή του επιλεγμένου έτους (ή της νεότερης αν δεν υπάρχει τρέχον), χωρίς YY."""
    toks = parse_cell_entries(cell_text)
    chosen = select_single_token(toks, current_year)
    if chosen is None:
        return ""
    _y, p = chosen
    return "" if p is None else (f"{p:g}")

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


# --- Helpers for dev token parsing/serialization (for per-year/month files) ---
def parse_dev_tokens(cell: str) -> list[dict]:
    """Parse tokens of form 100:2024;APRIL into dicts."""
    if not cell or not isinstance(cell, str):
        return []
    toks = []
    for t in cell.split(","):
        t = t.strip()
        m = TOKEN_DEV_RE.match(t)
        if m:
            toks.append({
                "price": float(m.group(1)),
                "year": int(m.group(2)),
                "month_en": m.group(3).upper(),
            })
    return toks

def serialize_dev(toks: list[dict]) -> str:
    return ",".join(f"{float(e['price']):g}:{int(e['year'])};{e['month_en'].upper()}" for e in toks if "price" in e and "year" in e and "month_en" in e)

def dedupe_by_key(toks: list[dict]) -> list[dict]:
    # Remove duplicates by (year, month_en): keep only the last token for each (year, month_en)
    keyed = {}
    for e in toks:
        k = (int(e["year"]), e["month_en"].upper())
        keyed[k] = e
    return list(keyed.values())

# Return the single price (as string) for the given year & month_en from a cell's tokens
def display_price_for_year_month(cell_text: str, year: int, month_en: str) -> str:
    toks = parse_dev_tokens(str(cell_text or ""))
    for e in toks:
        if int(e["year"]) == int(year) and e["month_en"].upper() == month_en.upper():
            return f"{e['price']:g}"
    return ""

def load_grid_df_for_year(year: int) -> pd.DataFrame:
    grid = empty_grid()
    # For each month column (Ισόγειο/Α/Β) try to read dev_{year}_{MONTHEN}.xlsx
    for m in MONTHS:
        m_en = month_en_of(m)
        fpath = dev_path_for(year, m_en)
        if not fpath.exists():
            continue
        try:
            df = pd.read_excel(fpath, sheet_name="grid")
        except Exception:
            continue
        if "Ημέρα" in df.columns:
            df = df.set_index("Ημέρα")
        # Expect columns exactly ["Ισόγειο", "Α", "Β"]
        for floor in FLOORS_DISPLAY:
            colname = f"{m} {floor}"
            if floor in df.columns:
                # normalize + dedupe by key just in case
                col_series = df[floor].astype("string").reindex(index=DAYS).fillna("")
                cleaned = []
                for d in DAYS:
                    val = str(col_series.loc[d] or "").strip()
                    if val:
                        parts = [p.strip() for p in val.split(",") if p and p.strip()]
                        # ensure tokens are forced to current file's year-month key
                        toks = []
                        for p in parts:
                            # if user typed bare number, convert to token
                            mm = re.match(r"^\d+(?:\.\d+)?$", p)
                            if mm:
                                toks.append(f"{mm.group(0)}:{int(year)};{m_en}")
                            else:
                                # keep only if already correct year;month
                                mm2 = TOKEN_DEV_RE.match(p)
                                if mm2 and int(mm2.group(2)) == int(year) and mm2.group(3).upper() == m_en:
                                    toks.append(f"{float(mm2.group(1)):g}:{int(mm2.group(2))};{mm2.group(3).upper()}")
                        val = ",".join(toks)
                        val = serialize_dev(dedupe_by_key(parse_dev_tokens(val)))
                    cleaned.append(val)
                grid.loc[DAYS, colname] = cleaned
    return _norm_df(grid)


def _norm_df(df: pd.DataFrame) -> pd.DataFrame:
    # Εξαναγκάζουμε ίδια διάταξη/τύπους και άδειο string αντί για <NA>
    df2 = df.reindex(index=DAYS, columns=GRID_COLUMNS)
    df2 = df2.astype("string").fillna("")
    return df2

def save_grid_df_for_year(grid: pd.DataFrame, year: int) -> tuple[bool, str | None]:
    grid = _norm_df(grid).astype("string").fillna("")
    try:
        # Write every month of the selected year to its own dev_{YYYY}_{MONTH}.xlsx
        for m in MONTHS:
            m_en = month_en_of(m)
            fpath = dev_path_for(year, m_en)
            # Build a narrow frame for this month: Ημέρα + three floors
            out = pd.DataFrame(index=DAYS)
            out.index.name = "Ημέρα"
            for floor in FLOORS_DISPLAY:
                colname = f"{m} {floor}"
                # ensure tokens are normalized *to this year & month*
                col_vals = []
                for d in DAYS:
                    raw = str(grid.at[d, colname] or "").strip()
                    if raw == "":
                        col_vals.append("")
                        continue
                    # accept either bare number or tokens, but force to single key (year;month)
                    mnum = re.search(r"\d+(?:\.\d+)?", raw)
                    if mnum and raw.strip() == mnum.group(0):
                        token = f"{float(mnum.group(0)):g}:{int(year)};{m_en}"
                        col_vals.append(token)
                    else:
                        toks = parse_dev_tokens(raw)
                        # filter to this (year, month)
                        toks = [e for e in toks if int(e["year"]) == int(year) and e["month_en"].upper() == m_en]
                        toks = dedupe_by_key(toks)
                        col_vals.append(serialize_dev(toks))
                out[floor] = col_vals
            # write excel
            out = out.reset_index()
            with pd.ExcelWriter(fpath, engine="openpyxl") as xl:
                out.to_excel(xl, sheet_name="grid", index=False)
        # Also refresh combined bookings.xlsx by scanning all dev_{YYYY}_{MONTH}.xlsx files in DATA_DIR
        recs = []
        for fp in DATA_DIR.glob("dev_*_*.xlsx"):
            m = re.match(r"dev_(\d{4})_([A-Z]+)\.xlsx$", fp.name)
            if not m:
                continue
            y = int(m.group(1))
            month_en = m.group(2)
            # map EN back to GR for unified stats
            month_gr = next((gr for gr, en in MONTH_EN.items() if en == month_en), None)
            if not month_gr:
                continue
            try:
                dfm = pd.read_excel(fp, sheet_name="grid")
            except Exception:
                continue
            if "Ημέρα" in dfm.columns:
                dfm = dfm.set_index("Ημέρα")
            for floor in FLOORS_DISPLAY:
                if floor not in dfm.columns:
                    continue
                for day in DAYS:
                    val = str(dfm.at[day, floor] if day in dfm.index else "")
                    toks = dedupe_by_key(parse_dev_tokens(val))
                    for e in toks:
                        # each token is already bound to a unique (year;month)
                        recs.append({
                            "year": int(e["year"]),
                            "floor": floor,
                            "month": month_gr,
                            "day": int(day),
                            "price": float(e["price"]),
                        })
        bookings = pd.DataFrame(recs, columns=["year", "floor", "month", "day", "price"]).sort_values(["year", "month", "floor", "day"]) if recs else pd.DataFrame(columns=["year", "floor", "month", "day", "price"]) 
        with pd.ExcelWriter(BOOKINGS_XLSX, engine="openpyxl") as xl:
            bookings.to_excel(xl, sheet_name="bookings", index=False)
        return True, None
    except Exception as e:
        return False, str(e)

def _frames_equal(a: pd.DataFrame, b: pd.DataFrame) -> bool:
    a2 = _norm_df(a)
    b2 = _norm_df(b)
    return a2.equals(b2)

# ---------- Sidebar (λειτουργίες) ----------
with st.sidebar:
    # Ensure year-scoped grid for sidebar actions
    sidebar_year = st.radio("Έτος εργασίας (Sidebar)", [2022, 2023, 2024, 2025], index=2, horizontal=True)
    session_key = f"grid_df::{sidebar_year}"
    if session_key not in st.session_state:
        st.session_state[session_key] = load_grid_df_for_year(int(sidebar_year))

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
                base = st.session_state[session_key].copy()
                if merge_mode.startswith("Αντικατάσταση"):
                    st.session_state[session_key] = _norm_df(new_grid)   # for Αντικατάσταση
                else:
                    # Συγχώνευση: κρατάμε τα παλιά εκτός αν το νέο έχει μη κενό
                    merged = base.copy().astype("string")
                    for col in GRID_COLUMNS:
                        left = merged[col].fillna("")
                        right = new_grid[col].fillna("")
                        merged[col] = np.where(right.astype(str).str.strip() != "", right, left)
                    st.session_state[session_key] = _norm_df(merged)     # for Συγχώνευση
                st.success("Ο πίνακας ενημερώθηκε από το CSV στην τρέχουσα χρονιά. Μην ξεχάσεις να πατήσεις Αποθήκευση αν θες να γραφτεί στη βάση.")
        except Exception as e:
            st.error(f"Αποτυχία ανάγνωσης CSV: {e}")

    st.markdown("—")
    st.subheader("Καθαρισμός (στο επιλεγμένο έτος Sidebar)")
    month_to_clear = st.selectbox("Μήνας", MONTHS, key="clear_month_select")
    if st.button("🧹 Καθάρισε τον μήνα στο έτος", key="btn_clear_month_year_only"):
        base = st.session_state[session_key].copy()
        for f in FLOORS_DISPLAY:
            col = f"{month_to_clear} {f}"
            if col in base.columns:
                base.loc[:, col] = ""
        st.session_state[session_key] = _norm_df(base)
        st.success(f"Καθαρίστηκε ο {month_to_clear} στο {sidebar_year}. Πάτα Αποθήκευση στην κεντρική φόρμα.")

    if st.button("🧨 Καθάρισε ΟΛΟΥΣ τους μήνες στο έτος", key="btn_clear_all_months_year_only"):
        # clear only columns of this year in UI (the files are per-year so this is safe)
        st.session_state[session_key] = empty_grid()
        st.warning(f"Καθαρίστηκαν όλοι οι μήνες στο {sidebar_year}. Πάτα Αποθήκευση στην κεντρική φόρμα.")

# ---------- Πίνακας (HTML‑styled) με φόρμα αποθήκευσης ----------
main_tabs = st.tabs(["Καταχώρηση", "Στατιστικά"])  # δύο σελίδες: εισαγωγή & στατιστικά

with main_tabs[0]:
    # Reload grid whenever the selected year changes
    current_year = st.radio("Έτος καταχώρησης", [2022, 2023, 2024, 2025], index=2, horizontal=True)
    session_key = f"grid_df::{current_year}"
    if session_key not in st.session_state:
        st.session_state[session_key] = load_grid_df_for_year(int(current_year))

    st.markdown(
        """
    <div class="card">
      <h3>🗂️ Πίνακας Κρατήσεων (Μήνας × Όροφος: Ισόγειο/Α/Β)</h3>
      <div class="small-muted">Οι αλλαγές καταχωρούνται όταν πατήσεις <strong>Αποθήκευση</strong> στο τέλος.</div>
    </div>
    """,
        unsafe_allow_html=True,
    )

    yy_current = int(current_year) % 100
    st.caption("Αν γράψεις μόνο αριθμούς (π.χ. 100), θα θεωρηθεί τιμή για το επιλεγμένο έτος.")

    def _label(month: str, floor: str, day: int) -> str:
        return f"{month} {floor} — {day}"

    with st.form("booking_form", clear_on_submit=False):
        tabs = st.tabs(MONTHS)
        new_values = {}
        for i, m in enumerate(MONTHS):
            with tabs[i]:
                st.markdown(f"### {m}")
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
                        raw_initial = st.session_state[session_key].at[d, colname] if (d in st.session_state[session_key].index and colname in st.session_state[session_key].columns) else ""
                        month_en = MONTH_EN[m]
                        initial = display_price_for_year_month(str(raw_initial or ""), int(current_year), month_en)
                        key = f"cell::{m}::{f}::{d}"
                        val = cols[j].text_input(_label(m, f, d), value="", key=key, placeholder=str(initial or ""), label_visibility="collapsed")
                        new_values[(d, colname)] = val
        submitted = st.form_submit_button("💾 Αποθήκευση", type="primary")

    if submitted:
        updated = st.session_state[session_key].copy()
        for (d, colname), v in new_values.items():
            if colname not in updated.columns or d not in updated.index:
                continue
            new_text = str(v or "").strip()
            if new_text == "":
                continue  # leave cell unchanged
            mnum = re.search(r"\d+(?:\.\d+)?", new_text)
            if not mnum:
                continue
            price_val = float(mnum.group(0))
            month_gr, _floor = split_month_floor(colname)
            m_en = MONTH_EN[month_gr]
            existing = parse_dev_tokens(str(updated.at[d, colname] or ""))
            # keep only tokens NOT matching this (year;month)
            existing = [e for e in existing if not (int(e["year"]) == int(current_year) and e["month_en"].upper() == m_en)]
            # add the new token for this exact key
            existing.append({"year": int(current_year), "month_en": m_en, "price": price_val})
            updated.at[d, colname] = serialize_dev(dedupe_by_key(existing))

        st.session_state[session_key] = updated.astype("string").fillna("")
        ok, err = save_grid_df_for_year(st.session_state[session_key], int(current_year))
        if ok:
            st.success("Αποθηκεύτηκαν οι κρατήσεις.")
        else:
            st.error(f"Σφάλμα αποθήκευσης: {err}")

        # Note for per-month files (above download buttons)
        st.info("Για το επιλεγμένο έτος δημιουργήθηκαν/ενημερώθηκαν αρχεία ανά μήνα: dev_{YYYY}_{MONTH}.xlsx. Το bookings.xlsx είναι ο ενιαίος πίνακας για όλα τα έτη/μήνες.")

        # Προσφέρουμε export μετά την επιτυχή αποθήκευση
        if ok:
            if BOOKINGS_XLSX.exists():
                bookings = pd.read_excel(BOOKINGS_XLSX, sheet_name="bookings")
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
