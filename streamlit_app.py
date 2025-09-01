# streamlit_app.py
# -*- coding: utf-8 -*-
"""
Εφαρμογή κρατήσεων (Ιανουάριος–Δεκέμβριος) με όμορφο UI και μόνιμη αποθήκευση σε SQLite.
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
import openpyxl

APP_TITLE = "📅 Κρατήσεις Διαμερισμάτων (Ιαν–Δεκ)"
# Χρησιμοποιούμε επίμονο φάκελο στο Streamlit Cloud (/mount/data) αν υπάρχει/είναι εγγράψιμος
_DATA_DIR = Path("/mount/data")
if _DATA_DIR.exists() and os.access(_DATA_DIR, os.W_OK):
    DB_PATH = _DATA_DIR / "bookings.db"
else:
    DB_PATH = Path("bookings.db")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

DAYS = list(range(1, 32))  # 1–31

MONTHS = [
    "Ιανουάριος",
    "Φεβρουάριος",
    "Μάρτιος",
    "Απρίλιος",
    "Μάιος",
    "Ιούνιος",
    "Ιούλιος",
    "Αύγουστος",
    "Σεπτέμβριος",
    "Οκτώβριος",
    "Νοέμβριος",
    "Δεκέμβριος",
]
DAYS = list(range(1, 32))  # 1–31


# Μήνες EN για αρχεία (όνομα αρχείου)
MONTH_EN = {
    "Ιανουάριος": "JANUARY",
    "Φεβρουάριος": "FEBRUARY",
    "Μάρτιος": "MARCH",
    "Απρίλιος": "APRIL",
    "Μάιος": "MAY",
    "Ιούνιος": "JUNE",
    "Ιούλιος": "JULY",
    "Αύγουστος": "AUGUST",
    "Σεπτέμβριος": "SEPTEMBER",
    "Οκτώβριος": "OCTOBER",
    "Νοέμβριος": "NOVEMBER",
    "Δεκέμβριος": "DECEMBER",
}
# Reverse map EN -> GR for imports that may use EN names
MONTH_GR_FROM_EN = {en: gr for gr, en in MONTH_EN.items()}
# Αριθμητικός χάρτης μηνών (GR -> 1..12)
MONTH_NUM = {m: i+1 for i, m in enumerate(MONTHS)}

# Όροφοι (εμφάνιση)
FLOORS_DISPLAY = ["Ισόγειο", "Α", "Β"]
# For expenses we also allow a general, non-floor-specific bucket
EXPENSE_FLOORS_DISPLAY = ["Ισόγειο", "Α", "Β", "Γενικά"]

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
MONTHLY_EXPENSE_XLSX = DATA_DIR / "monthly_expense.xlsx"

# ---- Combined writer: write both bookings & expenses into bookings.xlsx ----
def build_expenses_long() -> pd.DataFrame:
    """Scan monthly_expense.xlsx (all sheets by year) and return long-format expenses.
    Columns: year, floor, month (GR), price (float), price_token (str).
    Accept either bare numbers or tokens; normalize to token 'price:YYYY;MONTH;EX'.
    """
    cols = ["year", "floor", "month", "price", "price_token"]
    if not MONTHLY_EXPENSE_XLSX.exists():
        return pd.DataFrame(columns=cols)
    try:
        xls = pd.read_excel(MONTHLY_EXPENSE_XLSX, sheet_name=None)
    except Exception:
        return pd.DataFrame(columns=cols)
    recs = []
    for sheet_name, df in xls.items():
        try:
            y = int(sheet_name)
        except Exception:
            # skip non-year sheets
            continue
        cur = df.copy()
        if "Μήνας" in cur.columns:
            cur = cur.set_index("Μήνας")
        for m_gr in MONTHS:
            for f in EXPENSE_FLOORS_DISPLAY:
                val = str(cur.at[m_gr, f] if (m_gr in cur.index and f in cur.columns) else "").strip()
                if not val:
                    continue
                # Normalize: if bare number, convert to token; if token, keep if EX
                month_en = MONTH_EN[m_gr]
                mnum = re.match(r"^\d+(?:\.\d+)?$", val)
                token = None
                price_val = None
                if mnum:
                    price_val = float(mnum.group(0))
                    token = f"{price_val:g}:{int(y)};{month_en};EX"
                else:
                    mm = TOKEN_DEV_RE.match(val)
                    if mm:
                        price_val = float(mm.group(1))
                        y2 = int(mm.group(2))
                        men = mm.group(3).upper()
                        kind = (mm.group(4) or "").upper()
                        if kind == "EX":
                            # Only accept if the month matches sheet row
                            if men != month_en:
                                continue
                            token = f"{price_val:g}:{y2};{men};EX"
                            y = y2
                        else:
                            continue  # skip revenue tokens
                if token is None:
                    continue
                recs.append({
                    "year": int(y),
                    "floor": f,
                    "month": m_gr,
                    "price": float(price_val),
                    "price_token": token,
                })
    if not recs:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame(recs, columns=cols).sort_values(["year", "month", "floor"]).reset_index(drop=True)


def write_combined_excel(bookings_df: pd.DataFrame) -> None:
    """Write combined Excel (bookings + expenses) to BOOKINGS_XLSX.
    - Sheet 'bookings': provided bookings_df (may be empty but with proper columns).
    - Sheet 'expenses': long-format built from monthly_expense.xlsx (year, floor, month, price, price_token).
    """
    try:
        expenses_long = build_expenses_long()
        with pd.ExcelWriter(BOOKINGS_XLSX, engine="openpyxl") as xl:
            (bookings_df if bookings_df is not None else pd.DataFrame(columns=["year","floor","month","day","price"])) \
                .to_excel(xl, sheet_name="bookings", index=False)
            # Save expenses in the combined file; both numeric price and token are useful.
            (expenses_long if not expenses_long.empty else pd.DataFrame(columns=["year","floor","month","price","price_token"])) \
                .to_excel(xl, sheet_name="expenses", index=False)
    except Exception:
        # Fail silently to avoid crashing the app when only one part is available
        pass

def load_bookings_df() -> pd.DataFrame:
    """Load combined bookings for statistics from bookings.xlsx only."""
    cols = ["year", "floor", "month", "day", "price"]
    if BOOKINGS_XLSX.exists():
        try:
            df = pd.read_excel(BOOKINGS_XLSX, sheet_name="bookings")
            for c in cols:
                if c not in df.columns:
                    df[c] = pd.Series(dtype="float64" if c in ("year","day","price") else "string")
            return df[cols]
        except Exception:
            pass
    return pd.DataFrame(columns=cols)
TOKEN_DEV_RE = re.compile(r"^(\d+(?:\.\d+)?):(\d{4});([A-Z]+)(?:;(EX))?$")

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
    "<p class='small-muted'>Πληκτρολόγησε μόνο την <code>τιμή</code>. Η εφαρμογή τη γράφει ως token <code>τιμή:YYYY;MONTH</code> για έσοδα και <code>τιμή:YYYY;MONTH;EX</code> για έξοδα (π.χ. <code>80:2023;AUGUST</code>, <code>50:2024;MAY;EX</code>). Κάθε κελί δέχεται πολλά tokens χωρισμένα με κόμμα, αλλά το κλειδί <code>YYYY;MONTH</code> είναι μοναδικό: η νέα τιμή αντικαθιστά την παλιά για το ίδιο έτος/μήνα.</p>",
    unsafe_allow_html=True,
)

# Unified year selection shared across the app
if "selected_year" not in st.session_state:
    st.session_state["selected_year"] = 2024  # default

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
    """Parse tokens of form 100:2024;APRIL[;EX] into dicts with kind (REV/EX)."""
    if not cell or not isinstance(cell, str):
        return []
    toks = []
    for t in cell.split(","):
        t = t.strip()
        m = TOKEN_DEV_RE.match(t)
        if m:
            price = float(m.group(1))
            year = int(m.group(2))
            month_en = m.group(3).upper()
            kind = "EX" if (m.group(4) and m.group(4).upper() == "EX") else "REV"
            toks.append({"price": price, "year": year, "month_en": month_en, "kind": kind})
    return toks

def serialize_dev(toks: list[dict]) -> str:
    return ",".join(
        f"{float(e['price']):g}:{int(e['year'])};{e['month_en'].upper()}"
        + (";EX" if str(e.get('kind','REV')).upper()=="EX" else "")
        for e in toks if "price" in e and "year" in e and "month_en" in e
    )

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
# ===================== Helpers for Monthly Expenses =====================
def load_monthly_expense_df(year: int) -> pd.DataFrame:
    df = pd.DataFrame("", index=MONTHS, columns=EXPENSE_FLOORS_DISPLAY, dtype="string")
    df.index.name = "Μήνας"
    if MONTHLY_EXPENSE_XLSX.exists():
        try:
            xls = pd.read_excel(MONTHLY_EXPENSE_XLSX, sheet_name=None)
            sheet = str(int(year))
            if sheet in xls:
                cur = xls[sheet]
                if "Μήνας" in cur.columns:
                    cur = cur.set_index("Μήνας")
                for f in EXPENSE_FLOORS_DISPLAY:
                    if f in cur.columns:
                        df.loc[MONTHS, f] = cur[f].astype("string").reindex(MONTHS).fillna("")
        except Exception:
            pass
    return df

def save_monthly_expense_df(year: int, df: pd.DataFrame) -> tuple[bool, str | None]:
    try:
        book = {}
        if MONTHLY_EXPENSE_XLSX.exists():
            try:
                book = pd.read_excel(MONTHLY_EXPENSE_XLSX, sheet_name=None)
            except Exception:
                book = {}
        out = df.copy().reset_index()
        out.index.name = None
        book[str(int(year))] = out
        with pd.ExcelWriter(MONTHLY_EXPENSE_XLSX, engine="openpyxl") as xl:
            for name, frame in book.items():
                frame.to_excel(xl, sheet_name=name, index=False)
        return True, None
    except Exception as e:
        return False, str(e)

def display_expense_for_year_month(cell_text: str, year: int, month_en: str) -> str:
    toks = parse_dev_tokens(str(cell_text or ""))
    for e in toks:
        if int(e.get("year", 0)) == int(year) and str(e.get("month_en","")).upper()==month_en.upper() and str(e.get("kind","")).upper()=="EX":
            return f"{float(e['price']):g}"
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
        # Write combined (bookings + expenses) to bookings.xlsx
        write_combined_excel(bookings)
        return True, None
    except Exception as e:
        return False, str(e)

def _frames_equal(a: pd.DataFrame, b: pd.DataFrame) -> bool:
    a2 = _norm_df(a)
    b2 = _norm_df(b)
    return a2.equals(b2)

# --- UI helper: small explanatory note under charts ---
def explain(text: str):
    st.caption(text)

# ---------- Sidebar (λειτουργίες) ----------
with st.sidebar:
    st.header("ℹ️ Οδηγίες")
    st.markdown(
        "Πληκτρολόγησε **μόνο τιμή** (π.χ. 80). Η εφαρμογή την αποθηκεύει ως token **τιμή:YYYY;MONTH** (π.χ. 80:2023;AUGUST).\n\n"
        "Οι αλλαγές **δεν** αποθηκεύονται αυτόματα — πάτησε **Αποθήκευση** στο κάτω μέρος του πίνακα.")
    st.markdown("—")

    st.subheader("Εισαγωγή από Excel")
    import_year = st.selectbox("Έτος (για εισαγωγή)", [2022, 2023, 2024, 2025], index=2, key="import_year_select")
    import_multi_years = st.checkbox(
        "Ανίχνευση & εφαρμογή για ΟΛΑ τα έτη/μήνες (αν υπάρχουν στο αρχείο)",
        value=True,
        help="Αν το αρχείο περιέχει δεδομένα για πολλά έτη/μήνες, θα ενημερωθούν αυτόματα οι αντίστοιχοι πίνακες. Αν όχι, χρησιμοποιείται το έτος εισαγωγής."
    )
    session_key_import = f"grid_df::{import_year}"
    if session_key_import not in st.session_state:
        st.session_state[session_key_import] = load_grid_df_for_year(int(import_year))

    up = st.file_uploader(
        "Επίλεξε Excel",
        type=["xlsx", "xls"],
        help=(
            "Δέχεται είτε long-format (στήλες: year, floor, month, day, price) στο πρώτο φύλλο, "
            "είτε grid-format με στήλες τύπου 'Μάιος Ισόγειο', 'Μάιος Α', 'Μάιος Β' και προαιρετική στήλη 'Ημέρα'."
        ),
    )
    st.caption("Μπορείς να ανεβάσεις πλήρη βάση (όλα τα έτη/μήνες). Το σύστημα θα εφαρμόσει μόνο ό,τι ταιριάζει σε κάθε έτος/μήνα.")
    merge_mode = st.radio(
        "Τρόπος ενημέρωσης",
        ["Αντικατάσταση όλων", "Συγχώνευση (μόνο μη κενά)"],
        index=1,
        help=(
            "Αντικατάσταση: το αρχείο αντικαθιστά όλο το πλέγμα στο επιλεγμένο έτος.\n"
            "Συγχώνευση: μόνο τα μη κενά του αρχείου γράφουν πάνω στα υπάρχοντα."
        ),
    )

    def _ensure_session_grid(year: int) -> None:
        key = f"grid_df::{int(year)}"
        if key not in st.session_state:
            st.session_state[key] = load_grid_df_for_year(int(year))

    if up is not None and st.button("↪︎ Ενημέρωση πίνακα από Excel"):
        try:
            src = pd.read_excel(up, sheet_name=0)
            df = src.copy()
            cols_lower = {c.lower().strip(): c for c in df.columns}
            required_long = {"year", "floor", "month", "day"}
            is_long = required_long.issubset(set(cols_lower.keys()))

            pending: dict[int, pd.DataFrame] = {}
            years_found: set[int] = set()
            def _get_pending_grid(y: int) -> pd.DataFrame:
                if y not in pending:
                    pending[y] = empty_grid()
                return pending[y]

            if is_long:
                df = df.rename(columns={v: k for k, v in cols_lower.items()})
                for _, r in df.iterrows():
                    try:
                        year_val = r.get("year")
                        if pd.isna(year_val):
                            continue
                        year = int(year_val)
                        floor = str(r.get("floor")).strip()
                        month_raw = str(r.get("month")).strip()
                        day = int(r.get("day")) if not pd.isna(r.get("day")) else None
                        if floor not in FLOORS_DISPLAY or day not in DAYS:
                            continue
                        # Resolve month (accept GR or EN)
                        if month_raw in MONTHS:
                            month_gr = month_raw
                            month_en = MONTH_EN[month_gr]
                        else:
                            month_en_u = month_raw.upper()
                            month_gr = MONTH_GR_FROM_EN.get(month_en_u)
                            if not month_gr:
                                continue
                            month_en = month_en_u
                        # Price
                        if "price" in df.columns and not pd.isna(r.get("price")):
                            price_val = float(r.get("price"))
                        else:
                            continue
                        # Accumulate into pending grid for this year
                        years_found.add(year)
                        grid_y = _get_pending_grid(year)
                        col = f"{month_gr} {floor}"
                        prev = str(grid_y.at[day, col] or "").strip()
                        token = f"{price_val:g}:{int(year)};{month_en}"
                        grid_y.at[day, col] = (prev + ("," if prev else "") + token)
                    except Exception:
                        continue
            else:
                if "Ημέρα" in df.columns:
                    df = df.set_index("Ημέρα")
                keep_cols = [c for c in df.columns if c in GRID_COLUMNS]
                if not keep_cols:
                    st.error("Το Excel δεν αναγνωρίστηκε (ούτε long-format ούτε grid-format με σωστές στήλες).")
                else:
                    tmp = empty_grid()
                    tmp.loc[tmp.index, keep_cols] = df[keep_cols].astype("string").reindex(index=DAYS).fillna("")
                    for d in DAYS:
                        for col in keep_cols:
                            raw = str(tmp.at[d, col] or "").strip()
                            if raw == "":
                                continue
                            month_gr, _floor = split_month_floor(col)
                            month_en = MONTH_EN[month_gr]
                            parts = [p.strip() for p in raw.split(",") if p and p.strip()]
                            for p in parts:
                                m_full = TOKEN_DEV_RE.match(p)
                                if m_full:
                                    price_v = float(m_full.group(1))
                                    yr_v = int(m_full.group(2))
                                    mon_v = m_full.group(3).upper()
                                    # Only accept tokens whose MONTH matches the column's month
                                    if mon_v != month_en:
                                        continue
                                    years_found.add(yr_v)
                                    grid_y = _get_pending_grid(yr_v)
                                    colname = f"{month_gr} {_floor}"
                                    prev = str(grid_y.at[d, colname] or "").strip()
                                    token = f"{price_v:g}:{int(yr_v)};{month_en}"
                                    grid_y.at[d, colname] = (prev + ("," if prev else "") + token)
                                else:
                                    # Bare number: fall back to import_year
                                    if re.match(r"^\d+(?:\.\d+)?$", p):
                                        yr_v = int(import_year)
                                        years_found.add(yr_v)
                                        grid_y = _get_pending_grid(yr_v)
                                        colname = f"{month_gr} {_floor}"
                                        prev = str(grid_y.at[d, colname] or "").strip()
                                        token = f"{float(p):g}:{int(yr_v)};{month_en}"
                                        grid_y.at[d, colname] = (prev + ("," if prev else "") + token)
                                    # otherwise ignore malformed token

            # If nothing detected for multiple years and the user disabled multi-year, bind to import_year if any grid exists
            if not years_found and not import_multi_years:
                # ensure at least an empty grid exists under import_year to trigger the merge logic below
                pending[int(import_year)] = pending.get(int(import_year), empty_grid())
                years_found.add(int(import_year))

            # Apply to session_state per year
            updated_years = []
            for y, new_grid in pending.items():
                _ensure_session_grid(int(y))
                base = st.session_state[f"grid_df::{int(y)}"].copy()
                # Normalize new_grid just in case
                new_grid = _norm_df(new_grid)
                if merge_mode.startswith("Αντικατάσταση"):
                    st.session_state[f"grid_df::{int(y)}"] = new_grid
                else:
                    merged = base.copy().astype("string")
                    for col in GRID_COLUMNS:
                        left = merged[col].fillna("")
                        right = new_grid[col].fillna("")
                        merged[col] = np.where(right.astype(str).str.strip() != "", right, left)
                    st.session_state[f"grid_df::{int(y)}"] = _norm_df(merged)
                updated_years.append(int(y))

            if updated_years:
                updated_years = sorted(set(updated_years))
                errors = []
                for y in updated_years:
                    key_y = f"grid_df::{int(y)}"
                    ok, err = save_grid_df_for_year(st.session_state[key_y], int(y))
                    if not ok and err:
                        errors.append(f"{y}: {err}")
                if errors:
                    st.error("Ενημερώθηκαν αλλά **ΟΧΙ όλες** αποθηκεύτηκαν λόγω σφαλμάτων στα έτη: " + "; ".join(errors))
                else:
                    st.success("Ενημερώθηκαν **και αποθηκεύτηκαν** πίνακες για έτη: " + ", ".join(str(x) for x in updated_years) + ". Το ενιαίο bookings.xlsx ανανεώθηκε.")
                    if BOOKINGS_XLSX.exists():
                        st.download_button(
                            "⬇️ Λήψη bookings.xlsx",
                            data=open(BOOKINGS_XLSX, "rb").read(),
                            file_name="bookings.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="dl_bookings_after_import",
                        )
            else:
                st.warning("Δεν ανιχνεύθηκαν έγκυρα δεδομένα για ενημέρωση.")
        except Exception as e:
            st.error(f"Αποτυχία ανάγνωσης Excel: {e}")

    st.markdown("—")
    st.subheader("Εισαγωγή Εξόδων από Excel")
    expense_import_year = st.selectbox(
        "Έτος (fallback για bare numbers)",
        [2022, 2023, 2024, 2025],
        index=[2022, 2023, 2024, 2025].index(2024),
        key="expense_import_year_select",
        help="Αν το αρχείο έχει σκέτους αριθμούς χωρίς έτος, θα χρησιμοποιηθεί αυτό το έτος."
    )
    import_multi_years_exp = st.checkbox(
        "Ανίχνευση & εφαρμογή για ΟΛΑ τα έτη/μήνες (αν υπάρχουν στο αρχείο)",
        value=True,
        key="import_multi_years_exp",
        help="Αν το αρχείο περιέχει δεδομένα για πολλά έτη/μήνες, θα ενημερωθούν αυτόματα τα αντίστοιχα έτη. Αν όχι, χρησιμοποιείται το έτος fallback."
    )

    up_exp = st.file_uploader(
        "Επίλεξε Excel εξόδων",
        type=["xlsx", "xls"],
        key="expense_file_uploader",
        help=(
            "Δέχεται είτε grid-format με στήλες 'Ισόγειο', 'Α', 'Β', 'Γενικά' και γραμμές τους μήνες (προαιρετική στήλη 'Μήνας'), "
            "είτε long-format με στήλες: month|Μήνας (GR ή EN), floor (Ισόγειο/Α/Β/Γενικά), price (αριθμός)."
        ),
    )
    st.caption("Drag and drop file hereLimit 200MB per file • XLSX, XLS")
    merge_mode_exp = st.radio(
        "Τρόπος ενημέρωσης",
        ["Αντικατάσταση όλων", "Συγχώνευση (μόνο μη κενά)"],
        index=1,
        key="merge_mode_exp",
        help=(
            "Αντικατάσταση: το αρχείο αντικαθιστά όλο το πλέγμα εξόδων στο επιλεγμένο έτος.\n"
            "Συγχώνευση: μόνο τα μη κενά του αρχείου γράφουν πάνω στα υπάρχοντα."
        ),
    )

    if up_exp is not None and st.button("↪︎ Ενημέρωση εξόδων από Excel", key="btn_import_expenses_from_excel"):
        try:
            src_exp = pd.read_excel(up_exp, sheet_name=0)
            df_exp = src_exp.copy()
            # Normalize column names (lowercase trimmed)
            cols_lower = {c.lower().strip(): c for c in df_exp.columns}

            # Helpers
            def _empty_expense_grid():
                df0 = pd.DataFrame("", index=MONTHS, columns=EXPENSE_FLOORS_DISPLAY, dtype="string")
                df0.index.name = "Μήνας"
                return df0

            pending_exp: dict[int, pd.DataFrame] = {}
            years_found_exp: set[int] = set()

            def _get_out_df(y: int) -> pd.DataFrame:
                y = int(y)
                if y not in pending_exp:
                    if merge_mode_exp.startswith("Αντικατάσταση"):
                        pending_exp[y] = _empty_expense_grid()
                    else:
                        pending_exp[y] = load_monthly_expense_df(y)
                return pending_exp[y]

            # Detect long format
            is_long_exp = set(["floor", "price"]).issubset(set(cols_lower.keys())) and ("month" in cols_lower or "μήνας" in cols_lower)

            if is_long_exp:
                # Rename to unified keys
                df_exp = df_exp.rename(columns={
                    cols_lower.get("month", cols_lower.get("μήνας", "month")): "month_raw",
                    cols_lower["floor"]: "floor",
                    cols_lower["price"]: "price",
                })
                # Optional explicit year column
                has_year_col = "year" in cols_lower
                if has_year_col:
                    df_exp = df_exp.rename(columns={cols_lower["year"]: "year"})

                for _, r in df_exp.iterrows():
                    try:
                        month_val = str(r.get("month_raw")).strip()
                        floor_val = str(r.get("floor")).strip()
                        if floor_val not in EXPENSE_FLOORS_DISPLAY:
                            continue
                        # Resolve month (accept GR or EN)
                        if month_val in MONTHS:
                            month_gr = month_val
                            month_en = MONTH_EN[month_gr]
                        else:
                            month_en_u = month_val.upper()
                            month_gr = MONTH_GR_FROM_EN.get(month_en_u)
                            if not month_gr:
                                continue
                            month_en = month_en_u
                        # Determine year (fallback if not provided/NaN)
                        y = int(r.get("year")) if (has_year_col and not pd.isna(r.get("year"))) else int(expense_import_year)
                        # Read price (number); ignore non-numeric
                        if pd.isna(r.get("price")):
                            continue
                        price_val = float(r.get("price"))
                        token = f"{price_val:g}:{int(y)};{month_en};EX"
                        out_df_y = _get_out_df(y)
                        out_df_y.at[month_gr, floor_val] = token
                        years_found_exp.add(int(y))
                    except Exception:
                        continue
            else:
                # Grid-format: optional 'Μήνας' column as index; columns should include floors
                cur = df_exp.copy()
                if "Μήνας" in cur.columns:
                    cur = cur.set_index("Μήνας")
                for m_gr in MONTHS:
                    for f in EXPENSE_FLOORS_DISPLAY:
                        try:
                            val = str(cur.at[m_gr, f] if (m_gr in cur.index and f in cur.columns) else "").strip()
                        except Exception:
                            val = ""
                        if not val:
                            continue
                        month_en = MONTH_EN[m_gr]
                        # Accept either bare number or token; tokens may carry explicit year
                        mnum = re.match(r"^\d+(?:\.\d+)?$", val)
                        if mnum:
                            price_val = float(mnum.group(0))
                            y = int(expense_import_year)
                            out_df_y = _get_out_df(y)
                            out_df_y.at[m_gr, f] = f"{price_val:g}:{int(y)};{month_en};EX"
                            years_found_exp.add(int(y))
                        else:
                            mm = TOKEN_DEV_RE.match(val)
                            if mm:
                                price_val = float(mm.group(1))
                                y2 = int(mm.group(2)) if mm.group(2) else int(expense_import_year)
                                men = mm.group(3).upper()
                                kind = (mm.group(4) or "").upper()
                                if kind == "EX" and men == month_en:
                                    out_df_y = _get_out_df(y2)
                                    out_df_y.at[m_gr, f] = f"{price_val:g}:{int(y2)};{men};EX"
                                    years_found_exp.add(int(y2))
                            # ignore malformed tokens

            # If nothing detected for multiple years and the user disabled multi-year, bind to selected fallback year
            if not years_found_exp and not import_multi_years_exp:
                y = int(expense_import_year)
                pending_exp[y] = pending_exp.get(y, _get_out_df(y))
                years_found_exp.add(y)

            # Save per-year sheets and refresh combined workbook
            if pending_exp:
                errors = []
                for y, out_df in pending_exp.items():
                    ok_exp_imp, err_exp_imp = save_monthly_expense_df(int(y), out_df)
                    if not ok_exp_imp and err_exp_imp:
                        errors.append(f"{y}: {err_exp_imp}")
                try:
                    write_combined_excel(load_bookings_df())
                except Exception:
                    pass
                if errors:
                    st.error("Ενημερώθηκαν αλλά **ΟΧΙ όλα** τα έτη εξόδων λόγω σφαλμάτων: " + "; ".join(errors))
                else:
                    yrs = ", ".join(str(int(y)) for y in sorted(pending_exp.keys()))
                    st.success("Ενημερώθηκαν **και αποθηκεύτηκαν** τα μηνιαία έξοδα για έτη: " + yrs + ".")
                c1, c2 = st.columns(2)
                with c1:
                    if MONTHLY_EXPENSE_XLSX.exists():
                        st.download_button(
                            "⬇️ Λήψη monthly_expense.xlsx",
                            data=open(MONTHLY_EXPENSE_XLSX, "rb").read(),
                            file_name="monthly_expense.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="dl_monthly_expense_after_import",
                        )
                with c2:
                    if BOOKINGS_XLSX.exists():
                        st.download_button(
                            "⬇️ Λήψη bookings.xlsx (με φύλλο expenses)",
                            data=open(BOOKINGS_XLSX, "rb").read(),
                            file_name="bookings.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="dl_bookings_after_expense_import",
                        )
            else:
                st.warning("Δεν ανιχνεύθηκαν έγκυρα δεδομένα εξόδων για ενημέρωση.")
        except Exception as e:
            st.error(f"Αποτυχία ανάγνωσης Excel εξόδων: {e}")

    st.markdown("—")
    st.subheader("Καθαρισμός")
    clear_year = st.selectbox("Έτος", [2022, 2023, 2024, 2025], index=2, key="clear_year_select")
    session_key_clear = f"grid_df::{clear_year}"
    if session_key_clear not in st.session_state:
        st.session_state[session_key_clear] = load_grid_df_for_year(int(clear_year))

    month_to_clear = st.selectbox("Μήνας", MONTHS, key="clear_month_select")
    if st.button("🧹 Καθάρισε τον μήνα", key="btn_clear_month_year_only"):
        base = st.session_state[session_key_clear].copy()
        for f in FLOORS_DISPLAY:
            col = f"{month_to_clear} {f}"
            if col in base.columns:
                base.loc[:, col] = ""
        st.session_state[session_key_clear] = _norm_df(base)
        st.success(f"Καθαρίστηκε ο {month_to_clear} στο {clear_year}. Πάτα Αποθήκευση στην κεντρική φόρμα.")

    if st.button("🧨 Καθάρισε ΟΛΟΥΣ τους μήνες στο έτος", key="btn_clear_all_months_year_only"):
        st.session_state[session_key_clear] = empty_grid()
        st.warning(f"Καθαρίστηκαν όλοι οι μήνες στο {clear_year}. Πάτα Αποθήκευση στην κεντρική φόρμα.")

    st.markdown("—")
    st.subheader("Μαζικός καθαρισμός (ΟΛΑ τα έτη)")
    confirm_all_years = st.checkbox(
        "Είμαι σίγουρος/η ότι θέλω να καθαρίσω ΟΛΟΥΣ τους μήνες σε ΟΛΑ τα έτη",
        key="chk_clear_all_years",
    )
    do_clear_all = st.button(
        "🧨 Καθάρισε ΟΛΟΥΣ τους μήνες σε ΟΛΑ τα έτη",
        key="btn_clear_all_months_all_years",
        disabled=not confirm_all_years,
    )
    if do_clear_all:
        errors = []
        for y in [2022, 2023, 2024, 2025]:
            y_key = f"grid_df::{y}"
            st.session_state[y_key] = empty_grid()
            ok, err = save_grid_df_for_year(st.session_state[y_key], int(y))
            if not ok and err:
                errors.append(f"{y}: {err}")
        if errors:
            st.error("Ο καθαρισμός ολοκληρώθηκε με σφάλματα σε ορισμένα έτη: " + "; ".join(errors))
        else:
            st.success("Καθαρίστηκαν ΟΛΟΙ οι μήνες σε ΟΛΑ τα έτη (2022–2025). Το ενιαίο bookings αρχείο ανανεώθηκε.")

# ---------- Πίνακας (HTML‑styled) με φόρμα αποθήκευσης ----------
main_tabs = st.tabs(["Καταχώρηση Εσόδων", "Καταχώρηση Εξόδων", "Στατιστικά"])

with main_tabs[0]:
    # Reload grid whenever the selected year changes
    current_year = st.radio(
        "Έτος καταχώρησης",
        [2022, 2023, 2024, 2025],
        index=[2022, 2023, 2024, 2025].index(int(st.session_state.get("selected_year", 2024))),
        horizontal=True,
        key="selected_year",
    )
    current_year = int(st.session_state["selected_year"])  # ensure we use the unified state
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
        st.info("Για το επιλεγμένο έτος δημιουργήθηκαν/ενημερώθηκαν αρχεία ανά μήνα: dev_{YYYY}_{MONTH}.xlsx. Το bookings.xlsx είναι ο ενιαίος πίνακας για όλα τα έτη/μήνες και πάνω σε αυτό βασίζονται τα Στατιστικά.")

        # Προσφέρουμε export μετά την επιτυχή αποθήκευση
        if ok and BOOKINGS_XLSX.exists():
            st.download_button(
                "⬇️ Λήψη bookings.xlsx",
                data=open(BOOKINGS_XLSX, "rb").read(),
                file_name="bookings.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

with main_tabs[1]:
    # Monthly Expenses Entry (per month & floor)
    exp_year = st.radio(
        "Έτος εξόδων",
        [2022, 2023, 2024, 2025],
        index=[2022, 2023, 2024, 2025].index(int(st.session_state.get("selected_year", 2024))),
        horizontal=True,
        key="selected_expense_year",
    )
    exp_year = int(exp_year)
    exp_df = load_monthly_expense_df(exp_year)

    st.markdown(
        """
    <div class="card">
      <h3>💳 Καταχώρηση Εξόδων (Μηνιαία)</h3>
      <div class="small-muted">Για κάθε μήνα & όροφο καταχώρησε μία τιμή. Αποθηκεύεται ως <code>τιμή:YYYY;MONTH;EX</code>.</div>
    </div>
    """,
        unsafe_allow_html=True,
    )

    with st.form("expense_form", clear_on_submit=False):
        tabs_exp = st.tabs(MONTHS)
        new_exp_values = {}
        for i, m in enumerate(MONTHS):
            with tabs_exp[i]:
                st.markdown(f"### {m}")
                header_cols = st.columns([1, 1, 1, 1], gap="small")
                header_cols[0].markdown("<div class='col-header'>Ισόγειο</div>", unsafe_allow_html=True)
                header_cols[1].markdown("<div class='col-header'>Α</div>", unsafe_allow_html=True)
                header_cols[2].markdown("<div class='col-header'>Β</div>", unsafe_allow_html=True)
                header_cols[3].markdown("<div class='col-header'>Γενικά</div>", unsafe_allow_html=True)

                row = st.columns([1, 1, 1, 1], gap="small")
                for j, f in enumerate(EXPENSE_FLOORS_DISPLAY):
                    month_en = MONTH_EN[m]
                    raw_initial = str(exp_df.at[m, f] if (m in exp_df.index and f in exp_df.columns) else "")
                    placeholder_val = display_expense_for_year_month(raw_initial, int(exp_year), month_en)
                    key = f"expense_cell::{m}::{f}::{exp_year}"
                    val = row[j].text_input(f"{m} {f}", value="", key=key, placeholder=str(placeholder_val or ""), label_visibility="collapsed")
                    new_exp_values[(m, f)] = val
        submitted_exp = st.form_submit_button("💾 Αποθήκευση Εξόδων", type="primary")

    if submitted_exp:
        out_df = load_monthly_expense_df(exp_year)
        for (month_gr, floor), val in new_exp_values.items():
            val_s = str(val or "").strip()
            if val_s == "":
                continue
            mnum = re.search(r"\d+(?:\.\d+)?", val_s)
            if not mnum:
                continue
            price_val = float(mnum.group(0))
            month_en = MONTH_EN[month_gr]
            token = f"{price_val:g}:{int(exp_year)};{month_en};EX"
            out_df.at[month_gr, floor] = token
        ok_exp, err_exp = save_monthly_expense_df(exp_year, out_df)
        if ok_exp:
            st.success("Αποθηκεύτηκαν τα μηνιαία έξοδα για το έτος.")
            # Refresh the combined workbook so it includes an up-to-date 'expenses' sheet
            try:
                # Pass current bookings (if any) to avoid dropping that sheet
                existing_bookings = load_bookings_df()
                write_combined_excel(existing_bookings)
            except Exception:
                pass
            c1, c2 = st.columns(2)
            with c1:
                if MONTHLY_EXPENSE_XLSX.exists():
                    st.download_button(
                        "⬇️ Λήψη monthly_expense.xlsx",
                        data=open(MONTHLY_EXPENSE_XLSX, "rb").read(),
                        file_name="monthly_expense.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
            with c2:
                if BOOKINGS_XLSX.exists():
                    st.download_button(
                        "⬇️ Λήψη bookings.xlsx (με φύλλο expenses)",
                        data=open(BOOKINGS_XLSX, "rb").read(),
                        file_name="bookings.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
        else:
            st.error(f"Σφάλμα αποθήκευσης εξόδων: {err_exp}")
# ---------- Στατιστικά (δεύτερη σελίδα) ----------
with main_tabs[2]:
    st.markdown(
        """
    <div class="card">
      <h3>📈 Στατιστικά Κρατήσεων</h3>
      <div class="small-muted">Τα στατιστικά βασίζονται στα δεδομένα που έχουν αποθηκευτεί στο bookings.xlsx.</div>
    </div>
    """,
        unsafe_allow_html=True,
    )
    stats_df = load_bookings_df()
    if stats_df.empty:
        st.info("Δεν υπάρχουν ακόμη αποθηκευμένες κρατήσεις (bookings.xlsx).")
    else:
        # Type safety / coercion
        stats_df["year"] = pd.to_numeric(stats_df["year"], errors="coerce").astype("Int64")
        stats_df["day"] = pd.to_numeric(stats_df["day"], errors="coerce").astype("Int64")
        stats_df["price"] = pd.to_numeric(stats_df["price"], errors="coerce")
        stats_df["floor"] = stats_df["floor"].astype("string")
        stats_df["month"] = stats_df["month"].astype("string")

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
                year_range = st.slider(
                    "Έτη (εύρος)",
                    min_value=int(y_min),
                    max_value=int(y_max),
                    value=(int(y_min), int(y_max)),
                    step=1,
                    key="stats_year_range",
                )
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
            explain("Πόσες κρατήσεις έγιναν ανά έτος. Βοηθά να δούμε τη συνολική τάση (αύξηση/μείωση).")
        else:
            st.info("Δεν υπάρχουν δεδομένα για το εύρος ετών/ορόφων που επέλεξες.")

        st.subheader("Κρατήσεις ανά έτος & όροφο")
        if not per_year_floor.empty:
            # Pivot για stacked visualization
            pv = per_year_floor.pivot(index="year", columns="floor", values="κρατήσεις").fillna(0)
            st.bar_chart(pv)
            explain("Πώς μοιράζονται οι κρατήσεις ανά όροφο κάθε χρόνο — εντοπίζουμε ποιος όροφος οδηγεί την κίνηση.")
        else:
            st.info("Δεν υπάρχουν δεδομένα για τους ορόφους που επέλεξες.")

        # Μηνιαία εποχικότητα (σωρευτικά)
        st.subheader("Κρατήσεις ανά μήνα (εποχικότητα)")
        per_month = fdf.groupby("month").size().reindex(MONTHS).fillna(0)
        st.line_chart(per_month)
        explain("Εποχικότητα στη ζήτηση: ποιοι μήνες έχουν περισσότερες/λιγότερες κρατήσεις.")

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
        explain("Ποιες ημέρες του μήνα γεμίζουν περισσότερο ανά μήνα — βοηθά για min-stay/προωθήσεις.")

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
            explain("Πού συγκεντρώνονται οι τιμές: χαμηλές/μεσαίες/υψηλές. Χρήσιμο για έλεγχο outliers.")

            st.subheader("Μέση τιμή ανά έτος")
            st.line_chart(price_mean.set_index("year"))
            explain("Πώς μεταβάλλεται η μέση τιμή κάθε χρόνο — δείκτης τιμολογιακής πολιτικής.")

            st.subheader("Έσοδα ανά έτος")
            st.bar_chart(revenue.set_index("year"))
            explain("Συνολικά έσοδα ανά έτος (με βάση τις τιμές). Βοηθά στον προϋπολογισμό.")

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

        # ===================== EXTRA STATS & GRAPHS =====================
        st.markdown("---")
        st.subheader("Έσοδα ανά μήνα (stacked ανά όροφο)")
        rev_mf = fdf.dropna(subset=["price"]).groupby(["year", "month", "floor"]) ["price"].sum().reset_index()
        if not rev_mf.empty:
            # Preserve month order
            rev_mf["month"] = pd.Categorical(rev_mf["month"], categories=MONTHS, ordered=True)
            st.vega_lite_chart(
                rev_mf,
                {
                    "mark": "bar",
                    "encoding": {
                        "x": {"field": "month", "type": "ordinal", "sort": MONTHS, "title": "Μήνας"},
                        "y": {"aggregate": "sum", "field": "price", "type": "quantitative", "title": "Έσοδα"},
                        "color": {"field": "floor", "type": "nominal", "title": "Όροφος"},
                        "column": {"field": "year", "type": "ordinal", "title": "Έτος"},
                        "tooltip": [
                            {"field": "year", "type": "ordinal", "title": "Έτος"},
                            {"field": "month", "type": "ordinal", "title": "Μήνας"},
                            {"field": "floor", "type": "nominal", "title": "Όροφος"},
                            {"aggregate": "sum", "field": "price", "type": "quantitative", "title": "Έσοδα"}
                        ]
                    },
                    "width": 280,
                    "height": 260
                },
                use_container_width=True,
            )
            explain("Μηνιαία έσοδα και συνεισφορά κάθε ορόφου. Εντοπίζουμε ποιος όροφος οδηγεί έσοδα ανά μήνα.")
        else:
            st.info("Δεν υπάρχουν έσοδα για να εμφανιστούν ανά μήνα.")

        st.subheader("Σύγκριση ανά μήνα μεταξύ ετών (έσοδα)")
        rev_my = fdf.dropna(subset=["price"]).groupby(["year", "month"]).price.sum().reset_index()
        if not rev_my.empty:
            rev_my["month"] = pd.Categorical(rev_my["month"], categories=MONTHS, ordered=True)
            st.vega_lite_chart(
                rev_my,
                {
                    "layer": [
                        {
                            "mark": "line",
                            "encoding": {
                                "x": {"field": "month", "type": "ordinal", "sort": MONTHS, "title": "Μήνας"},
                                "y": {"field": "price", "type": "quantitative", "title": "Έσοδα"},
                                "color": {"field": "year", "type": "nominal", "title": "Έτος"}
                            }
                        },
                        {
                            "mark": {"type": "point"},
                            "encoding": {
                                "x": {"field": "month", "type": "ordinal", "sort": MONTHS},
                                "y": {"field": "price", "type": "quantitative"},
                                "color": {"field": "year", "type": "nominal"}
                            }
                        },
                        {
                            "params": [{"name": "pick", "select": {"type": "point", "fields": ["month"], "on": "click", "nearest": True, "clear": "dblclick"}}],
                            "mark": {"type": "rule", "strokeDash": [4,4]},
                            "transform": [{"filter": {"param": "pick"}}],
                            "encoding": {
                                "x": {"field": "month", "type": "ordinal", "sort": MONTHS}
                            }
                        },
                        {
                            "mark": {"type": "text", "dy": -8},
                            "transform": [{"filter": {"param": "pick"}}],
                            "encoding": {
                                "x": {"field": "month", "type": "ordinal", "sort": MONTHS},
                                "y": {"field": "price", "type": "quantitative"},
                                "color": {"field": "year", "type": "nominal"},
                                "text": {"field": "price", "type": "quantitative", "format": ",.0f"}
                            }
                        }
                    ],
                    "width": "container",
                    "height": 260
                },
                use_container_width=True,
            )
            explain("Σύγκριση ίδιου μήνα μεταξύ ετών — βλέπουμε αν π.χ. ο Αύγουστος βελτιώθηκε σε σχέση με πέρσι.")

        st.subheader("Εβδομαδιαίο μοτίβο (κρατήσεις ανά ημέρα εβδομάδας)")
        # Προετοιμασία ημερομηνίας & weekday (χωρίς system locale)
        fdf_dates = fdf.copy()
        fdf_dates["month_num"] = fdf_dates["month"].map(MONTH_NUM)
        fdf_dates["date"] = pd.to_datetime(
            dict(year=fdf_dates["year"], month=fdf_dates["month_num"], day=fdf_dates["day"]),
            errors="coerce",
        )
        fdf_dates = fdf_dates.dropna(subset=["date"])  # κρατάμε μόνο έγκυρες
        if not fdf_dates.empty:
            weekday_map_gr = {
                0: "Δευτέρα",
                1: "Τρίτη",
                2: "Τετάρτη",
                3: "Πέμπτη",
                4: "Παρασκευή",
                5: "Σάββατο",
                6: "Κυριακή",
            }
            fdf_dates["weekday_idx"] = fdf_dates["date"].dt.weekday
            fdf_dates["weekday"] = fdf_dates["weekday_idx"].map(weekday_map_gr)
            weekday_order = [
                "Δευτέρα", "Τρίτη", "Τετάρτη", "Πέμπτη", "Παρασκευή", "Σάββατο", "Κυριακή"
            ]
            wdf = fdf_dates.groupby(["weekday"]).size().reset_index(name="κρατήσεις")
            wdf["weekday"] = pd.Categorical(wdf["weekday"], categories=weekday_order, ordered=True)
            st.bar_chart(wdf.set_index("weekday"))
            explain("Ποιες ημέρες εβδομάδας έχουν μεγαλύτερη ζήτηση — χρήσιμο για διαφοροποίηση τιμής ανά ημέρα.")
        else:
            st.info("Δεν ήταν δυνατή η δημιουργία έγκυρων ημερομηνιών για weekday ανάλυση.")

        st.subheader("Boxplot τιμών ανά μήνα")
        prices_month = fdf.dropna(subset=["price"]).copy()
        if not prices_month.empty:
            prices_month["month"] = pd.Categorical(prices_month["month"], categories=MONTHS, ordered=True)
            st.vega_lite_chart(
                prices_month,
                {
                    "mark": {"type": "boxplot"},
                    "encoding": {
                        "x": {"field": "month", "type": "ordinal", "sort": MONTHS, "title": "Μήνας"},
                        "y": {"field": "price", "type": "quantitative", "title": "Τιμή"},
                        "color": {"field": "month", "type": "nominal", "legend": None}
                    },
                    "width": "container",
                    "height": 260
                },
                use_container_width=True,
            )
            explain("Διασπορά τιμών ανά μήνα (median, εύρος). Βρίσκουμε μήνες με μεγάλες διακυμάνσεις.")
        else:
            st.info("Δεν υπάρχουν τιμές για boxplot.")

        st.subheader("Πληρότητα ανά μήνα (% ημερών με τουλάχιστον μία κράτηση)")
        # Για κάθε (έτος, μήνας, όροφος) μετράμε τις διαφορετικές ημέρες και το / σύνολο ημερών του μήνα
        occ = (
            fdf.groupby(["year", "month", "floor"]).agg(days_booked=("day", lambda s: s.dropna().nunique())).reset_index()
        )
        if not occ.empty:
            # Υπολογισμός συνολικών ημερών/μήνα ανά έτος
            occ["month_num"] = occ["month"].map(MONTH_NUM)
            import calendar
            occ["days_in_month"] = occ.apply(lambda r: calendar.monthrange(int(r["year"]), int(r["month_num"]))[1], axis=1)
            occ["occupancy"] = (occ["days_booked"] / occ["days_in_month"]) * 100
            occ["month"] = pd.Categorical(occ["month"], categories=MONTHS, ordered=True)
            # Επιλογή έτους για καθαρό γράφημα (ένα έτος τη φορά)
            years_occ = sorted(occ["year"].dropna().astype(int).unique().tolist())
            sel_year = st.selectbox("Έτος", years_occ, index=len(years_occ)-1, key="occ_year_select")
            occ_y = occ[occ["year"] == sel_year].copy()
            # Ταξινόμηση μηνών
            occ_y["month"] = pd.Categorical(occ_y["month"], categories=MONTHS, ordered=True)
            st.vega_lite_chart(
                occ_y,
                {
                    "layer": [
                        {
                            "mark": "line",
                            "encoding": {
                                "x": {"field": "month", "type": "ordinal", "sort": MONTHS, "title": "Μήνας"},
                                "y": {"field": "occupancy", "type": "quantitative", "title": "% Πληρότητα"},
                                "color": {"field": "floor", "type": "nominal", "title": "Όροφος"}
                            }
                        },
                        {
                            "mark": {"type": "point"},
                            "encoding": {
                                "x": {"field": "month", "type": "ordinal", "sort": MONTHS},
                                "y": {"field": "occupancy", "type": "quantitative"},
                                "color": {"field": "floor", "type": "nominal"}
                            }
                        },
                        {
                            "params": [{"name": "pick2", "select": {"type": "point", "fields": ["month"], "on": "click", "nearest": True, "clear": "dblclick"}}],
                            "mark": {"type": "rule", "strokeDash": [4,4]},
                            "transform": [{"filter": {"param": "pick2"}}],
                            "encoding": {
                                "x": {"field": "month", "type": "ordinal", "sort": MONTHS}
                            }
                        },
                        {
                            "mark": {"type": "text", "dy": -8},
                            "transform": [
                                {"filter": {"param": "pick2"}},
                                {"calculate": "datum.month + ' — ' + format(datum.occupancy, '.1f') + '%' + ' (' + datum.days_booked + '/' + datum.days_in_month + ')'"
                                , "as": "label_occ"}
                            ],
                            "encoding": {
                                "x": {"field": "month", "type": "ordinal", "sort": MONTHS},
                                "y": {"field": "occupancy", "type": "quantitative"},
                                "color": {"field": "floor", "type": "nominal"},
                                "text": {"field": "label_occ", "type": "nominal"}
                            }
                        }
                    ],
                    "width": "container",
                    "height": 260
                },
                use_container_width=True,
            )
            explain("Πληρότητα ανά μήνα για το επιλεγμένο έτος. Κάνε κλικ σε μήνα για ακριβές ποσοστό.")
        else:
            st.info("Δεν βρέθηκαν δεδομένα για υπολογισμό πληρότητας.")

        # ===================== EXTRA PLUS: CLIENT-FOCUSED STATS =====================
        st.markdown("---")
        st.subheader("Σωρευτικά έσοδα (YTD) ανά έτος")
        rev_month = fdf.dropna(subset=["price"]).groupby(["year", "month"]) ["price"].sum().reset_index()
        if not rev_month.empty:
            rev_month["month"] = pd.Categorical(rev_month["month"], categories=MONTHS, ordered=True)
            # Υπολογισμός σωρευτικών ανά έτος στην τάξη μηνών
            rev_month = rev_month.sort_values(["year", "month"]).copy()
            rev_month["ytd"] = rev_month.groupby("year")["price"].cumsum()
            st.vega_lite_chart(
                rev_month,
                {
                    "mark": "line",
                    "encoding": {
                        "x": {"field": "month", "type": "ordinal", "sort": MONTHS, "title": "Μήνας"},
                        "y": {"field": "ytd", "type": "quantitative", "title": "Σωρευτικά έσοδα"},
                        "color": {"field": "year", "type": "nominal", "title": "Έτος"},
                        "tooltip": [
                            {"field": "year", "type": "ordinal"},
                            {"field": "month", "type": "ordinal"},
                            {"field": "ytd", "type": "quantitative", "title": "YTD"}
                        ]
                    },
                    "width": "container",
                    "height": 280
                },
                use_container_width=True,
            )
            explain("Πόσο γρήγορα συσσωρεύονται τα έσοδα μέσα στη χρονιά — εύκολο benchmark μεταξύ ετών.")
        else:
            st.info("Δεν υπάρχουν δεδομένα εσόδων για σωρευτικό γράφημα.")

        st.subheader("Μερίδιο εσόδων ανά όροφο (mix) ανά μήνα")
        mix = fdf.dropna(subset=["price"]).groupby(["year", "month", "floor"]) ["price"].sum().reset_index()
        if not mix.empty:
            mix["month"] = pd.Categorical(mix["month"], categories=MONTHS, ordered=True)
            # Υπολογισμός ποσοστού ανά (έτος, μήνα)
            mix["total_month"] = mix.groupby(["year", "month"]) ["price"].transform("sum")
            mix["share"] = (mix["price"] / mix["total_month"]) * 100
            # Επιλογή έτους για καθαρό γράφημα (ένα έτος τη φορά)
            years_mix = sorted(mix["year"].dropna().astype(int).unique().tolist())
            sel_year_mix = st.selectbox("Έτος", years_mix, index=len(years_mix)-1, key="mix_year_select")
            mix_y = mix[mix["year"] == sel_year_mix].copy()
            st.vega_lite_chart(
                mix_y,
                {
                    "layer": [
                        {
                            "mark": "area",
                            "encoding": {
                                "x": {"field": "month", "type": "ordinal", "sort": MONTHS, "title": "Μήνας"},
                                "y": {"field": "share", "type": "quantitative", "stack": "normalize", "title": "% μερίδιο"},
                                "color": {"field": "floor", "type": "nominal", "title": "Όροφος"}
                            }
                        },
                        {
                            "mark": {"type": "point"},
                            "encoding": {
                                "x": {"field": "month", "type": "ordinal", "sort": MONTHS},
                                "y": {"field": "share", "type": "quantitative"},
                                "color": {"field": "floor", "type": "nominal"}
                            }
                        },
                        {
                            "params": [{"name": "pick_mix", "select": {"type": "point", "fields": ["month"], "on": "click", "nearest": True, "clear": "dblclick"}}],
                            "mark": {"type": "rule", "strokeDash": [4,4]},
                            "transform": [{"filter": {"param": "pick_mix"}}],
                            "encoding": {
                                "x": {"field": "month", "type": "ordinal", "sort": MONTHS}
                            }
                        },
                        {
                            "mark": {"type": "text", "dy": -8},
                            "transform": [
                                {"filter": {"param": "pick_mix"}},
                                {"calculate": "datum.month + ' — ' + format(datum.share, '.1f') + '%' + ' (' + format(datum.price, ',.0f') + ')'"
                                , "as": "label_mix"}
                            ],
                            "encoding": {
                                "x": {"field": "month", "type": "ordinal", "sort": MONTHS},
                                "y": {"field": "share", "type": "quantitative"},
                                "color": {"field": "floor", "type": "nominal"},
                                "text": {"field": "label_mix", "type": "nominal"}
                            }
                        }
                    ],
                    "width": "container",
                    "height": 260
                },
                use_container_width=True,
            )
            explain("Μερίδιο εσόδων ανά όροφο για το επιλεγμένο έτος. Κλικ σε μήνα για ακριβές ποσοστό.")
        else:
            st.info("Δεν υπάρχουν δεδομένα για μερίδιο εσόδων.")

        # ===================== EXPENSES & PROFITS =====================
        st.markdown("---")
        st.subheader("Έξοδα & Κέρδη")

        # Load expenses (long-format) from monthly_expense.xlsx via helper
        exp_long = build_expenses_long()
        if exp_long is None or exp_long.empty:
            st.info("Δεν υπάρχουν καταχωρημένα έξοδα (monthly_expense.xlsx).")
        else:
            # Coerce types and order months
            exp_long["year"] = pd.to_numeric(exp_long["year"], errors="coerce").astype("Int64")
            exp_long["price"] = pd.to_numeric(exp_long["price"], errors="coerce")
            exp_long["floor"] = exp_long["floor"].astype("string")
            exp_long["month"] = pd.Categorical(exp_long["month"], categories=MONTHS, ordered=True)

            # Filters (separate selection for expense categories so we can include 'Γενικά')
            exp_floors_sel = st.multiselect(
                "Κατηγορίες εξόδων",
                EXPENSE_FLOORS_DISPLAY,
                default=EXPENSE_FLOORS_DISPLAY,
                key="stats_expense_floors_sel",
            )
            exp_fdf = exp_long[exp_long["floor"].isin(exp_floors_sel)].copy()
            # Year range synchronized with bookings year_range if it exists
            if years_available:
                exp_fdf = exp_fdf[(exp_fdf["year"] >= year_range[0]) & (exp_fdf["year"] <= year_range[1])]

            # ---- KPIs for Expenses ----
            total_expenses = float(exp_fdf["price"].sum()) if not exp_fdf.empty else 0.0
            st.metric("Σύνολο εξόδων (φιλτραρισμένα)", f"{total_expenses:,.0f}")

            # ---- Expenses per Year ----
            st.subheader("Έξοδα ανά έτος")
            per_year_exp = exp_fdf.groupby("year")["price"].sum().reset_index().rename(columns={"price":"έξοδα"}) if not exp_fdf.empty else pd.DataFrame(columns=["year","έξοδα"]) 
            if not per_year_exp.empty:
                st.bar_chart(per_year_exp.set_index("year"))
                explain("Συνολικά έξοδα ανά έτος για τις επιλεγμένες κατηγορίες.")
            else:
                st.info("Δεν υπάρχουν έξοδα στο επιλεγμένο εύρος.")

            # ---- Expenses per Year & Floor (stacked) ----
            st.subheader("Έξοδα ανά έτος & κατηγορία")
            per_year_floor_exp = exp_fdf.groupby(["year","floor"])["price"].sum().reset_index()
            if not per_year_floor_exp.empty:
                pv_exp = per_year_floor_exp.pivot(index="year", columns="floor", values="price").fillna(0)
                st.bar_chart(pv_exp)
                explain("Συνεισφορά κάθε κατηγορίας (Ισόγειο/Α/Β/Γενικά) στα έξοδα ανά έτος.")
            else:
                st.info("Δεν υπάρχουν αναλυμένα έξοδα ανά κατηγορία.")

            # ---- Monthly Expenses Seasonality ----
            st.subheader("Έξοδα ανά μήνα (εποχικότητα)")
            per_month_exp = exp_fdf.groupby("month")["price"].sum().reindex(MONTHS).fillna(0)
            st.line_chart(per_month_exp)
            explain("Ποιοι μήνες επιβαρύνουν περισσότερο τα έξοδα.")

            # ===================== PROFITS =====================
            st.subheader("Κέρδη ανά έτος")
            # Revenue per year from filtered bookings (fdf built earlier)
            rev_per_year = (
                fdf.dropna(subset=["price"]).groupby("year")["price"].sum().reset_index().rename(columns={"price":"έσοδα"})
                if not fdf.empty else pd.DataFrame(columns=["year","έσοδα"]) 
            )
            # Align expenses per year to compare
            exp_per_year = per_year_exp.rename(columns={"έξοδα":"expenses"}) if "έξοδα" in per_year_exp.columns else pd.DataFrame(columns=["year","expenses"]) 
            prof = pd.merge(rev_per_year, exp_per_year, on="year", how="outer").fillna(0)
            if not prof.empty:
                prof["κέρδη"] = prof["έσοδα"] - prof["expenses"]
                # KPI for latest year in range
                if len(prof["year"].astype("Int64").dropna())>0:
                    last_year_prof = int(prof["year"].max())
                    val_profit = float(prof.loc[prof["year"]==last_year_prof, "κέρδη"].iloc[0])
                    st.metric(f"Κέρδη ({last_year_prof})", f"{val_profit:,.0f}")
                st.bar_chart(prof.set_index("year")["κέρδη"])
                explain("Κέρδη = Έσοδα − Έξοδα (περιλαμβάνονται τα 'Γενικά' έξοδα).")
            else:
                st.info("Δεν μπορούν να υπολογιστούν κέρδη χωρίς έσοδα/έξοδα.")

            # ---- Monthly Profit per Year ----
            st.subheader("Κέρδη ανά μήνα (ανά έτος)")
            rev_my = fdf.dropna(subset=["price"]).groupby(["year","month"]) ["price"].sum().reset_index().rename(columns={"price":"rev"}) if not fdf.empty else pd.DataFrame(columns=["year","month","rev"]) 
            exp_my = exp_fdf.groupby(["year","month"]) ["price"].sum().reset_index().rename(columns={"price":"exp"}) if not exp_fdf.empty else pd.DataFrame(columns=["year","month","exp"]) 
            pm = pd.merge(rev_my, exp_my, on=["year","month"], how="outer").fillna(0)
            if not pm.empty:
                pm["month"] = pd.Categorical(pm["month"], categories=MONTHS, ordered=True)
                pm = pm.sort_values(["year","month"]).copy()
                pm["profit"] = pm["rev"] - pm["exp"]
                # Facet by year for clarity
                st.vega_lite_chart(
                    pm,
                    {
                        "mark": "line",
                        "encoding": {
                            "x": {"field": "month", "type": "ordinal", "sort": MONTHS, "title": "Μήνας"},
                            "y": {"field": "profit", "type": "quantitative", "title": "Κέρδη"},
                            "color": {"field": "year", "type": "nominal", "title": "Έτος"}
                        },
                        "width": "container",
                        "height": 260
                    },
                    use_container_width=True,
                )
                explain("Κέρδη ανά μήνα: βοηθά να δεις σε ποιους μήνες τα έξοδα ροκανίζουν περισσότερο τα έσοδα.")
            else:
                st.info("Δεν υπάρχουν δεδομένα για μηνιαία κέρδη.")

        st.subheader("Μεταβολή εσόδων ανά μήνα σε σχέση με πέρσι (YoY)")
        yoy_years = sorted(fdf["year"].dropna().astype(int).unique().tolist())
        if len(yoy_years) >= 2:
            default_year = yoy_years[-1]
            compare_year = st.selectbox("Έτος προς ανάλυση", yoy_years, index=len(yoy_years)-1, key="yoy_target_year")
            base_year = st.selectbox("Σύγκριση με έτος", yoy_years[:-1], index=len(yoy_years)-2 if len(yoy_years) > 2 else 0, key="yoy_base_year")
            # Revenue per (year, month)
            yoy = fdf.dropna(subset=["price"]).groupby(["year", "month"]) ["price"].sum().reset_index()
            yoy["month"] = pd.Categorical(yoy["month"], categories=MONTHS, ordered=True)
            # Pivot to align months across two years
            piv = yoy.pivot_table(index="month", columns="year", values="price", aggfunc="sum").reindex(MONTHS)
            if compare_year in piv.columns and base_year in piv.columns:
                df_yoy = pd.DataFrame({
                    "month": MONTHS,
                    "delta": (piv[compare_year] - piv[base_year]).fillna(0),
                    "delta_pct": ((piv[compare_year] - piv[base_year]) / piv[base_year].replace({0: np.nan})) * 100,
                }).fillna(0)
                st.vega_lite_chart(
                    df_yoy,
                    {
                        "mark": "bar",
                        "encoding": {
                            "x": {"field": "month", "type": "ordinal", "sort": MONTHS, "title": "Μήνας"},
                            "y": {"field": "delta", "type": "quantitative", "title": f"Δ έσοδα: {compare_year} vs {base_year}"},
                            "color": {"condition": {"test": "datum.delta >= 0", "value": "#2ca02c"}, "value": "#d62728"},
                            "tooltip": [
                                {"field": "month", "type": "ordinal"},
                                {"field": "delta", "type": "quantitative", "title": "Δ έσοδα"},
                                {"field": "delta_pct", "type": "quantitative", "title": "Δ %"}
                            ]
                        },
                        "width": "container",
                        "height": 240
                    },
                    use_container_width=True,
                )
                explain("Διαφορά εσόδων έναντι βάσης (πέρσι). Θετικό=καλύτερα, αρνητικό=χειρότερα.")
            else:
                st.info("Δεν υπάρχουν και τα δύο έτη για πλήρη YoY σύγκριση.")
        else:
            st.info("Απαιτούνται τουλάχιστον δύο έτη για YoY.")

        # ------- Practical KPIs for a non-technical client -------
        st.markdown("---")
        st.subheader("Γρήγορα Συμπεράσματα (Auto-Insights)")
        try:
            insights = []
            # Best month by revenue (overall and per latest year)
            if not rev_month.empty:
                best_month_overall = (
                    rev_month.groupby("month")["price"].sum().sort_values(ascending=False).index[0]
                )
                insights.append(f"Καλύτερος μήνας συνολικά: **{best_month_overall}**.")
                ly = int(rev_month["year"].max())
                best_month_last = (
                    rev_month[rev_month["year"] == ly].sort_values("price", ascending=False)["month"].iloc[0]
                )
                insights.append(f"Καλύτερος μήνας στο **{ly}**: **{best_month_last}**.")
            # Best floor by revenue in latest year
            rev_floor_last = fdf.dropna(subset=["price"]).groupby(["year", "floor"]) ["price"].sum().reset_index()
            if not rev_floor_last.empty:
                ly = int(rev_floor_last["year"].max())
                sub = rev_floor_last[rev_floor_last["year"] == ly].sort_values("price", ascending=False)
                if not sub.empty:
                    insights.append(f"Κορυφαίος όροφος το **{ly}**: **{sub.iloc[0]['floor']}**.")
            # Occupancy best month (using days with ≥1 booking)
            occ2 = (
                fdf.groupby(["year", "month"]).agg(days_booked=("day", lambda s: s.dropna().nunique())).reset_index()
            )
            if not occ2.empty:
                occ2["month_num"] = occ2["month"].map(MONTH_NUM)
                import calendar
                occ2["days_in_month"] = occ2.apply(lambda r: calendar.monthrange(int(r["year"]), int(r["month_num"]))[1], axis=1)
                occ2["occ"] = (occ2["days_booked"] / occ2["days_in_month"]) * 100
                # pick latest year
                ly = int(occ2["year"].max())
                sub = occ2[occ2["year"] == ly].sort_values("occ", ascending=False)
                if not sub.empty:
                    insights.append(f"Μέγιστη πληρότητα στο **{ly}**: **{sub.iloc[0]['month']}** (~{sub.iloc[0]['occ']:.0f}%).")
            # Price median this year vs last
            years_ok = sorted(fdf["year"].dropna().unique().astype(int))
            if len(years_ok) >= 2 and fdf["price"].notna().any():
                y_now, y_prev = years_ok[-1], years_ok[-2]
                med_now = float(fdf[(fdf["year"] == y_now) & fdf["price"].notna()]["price"].median()) if not fdf[(fdf["year"] == y_now)]["price"].dropna().empty else np.nan
                med_prev = float(fdf[(fdf["year"] == y_prev) & fdf["price"].notna()]["price"].median()) if not fdf[(fdf["year"] == y_prev)]["price"].dropna().empty else np.nan
                if not np.isnan(med_now) and not np.isnan(med_prev) and med_prev != 0:
                    diff = med_now - med_prev
                    pct = (diff / med_prev) * 100
                    arrow = "↑" if diff >= 0 else "↓"
                    insights.append(f"Διάμεση τιμή **{y_now}** vs **{y_prev}**: {arrow} {abs(pct):.1f}%.")
            if insights:
                for s_ in insights:
                    st.write("• " + s_)
            else:
                st.info("Δεν προέκυψαν αυτόματα ευρήματα για τα επιλεγμένα φίλτρα.")
        except Exception as _e:
            st.info("Δεν ήταν δυνατή η παραγωγή auto-insights για τα τρέχοντα φίλτρα.")

        # ===================== VALUE-ADD WITH NO NEW DATA =====================
        st.markdown("---")
        st.subheader("Top / Bottom ημέρες (με βάση τιμή)")
        # Εύρεση κορυφαίων / χειρότερων ημερών από τα φιλτραρισμένα
        tbl_days = fdf.dropna(subset=["price"]).copy()
        if not tbl_days.empty:
            # Προστασία: με orders
            tbl_days["month"] = pd.Categorical(tbl_days["month"], categories=MONTHS, ordered=True)
            topn = int(st.number_input("Πλήθος (Top/Bottom)", min_value=3, max_value=50, value=10, step=1))
            best = tbl_days.sort_values("price", ascending=False).head(topn).copy()
            worst = tbl_days.sort_values("price", ascending=True).head(topn).copy()
            c1, c2 = st.columns(2)
            with c1:
                st.write("**Top ημέρες**")
                st.dataframe(best[["year", "month", "day", "floor", "price"]], use_container_width=True)
            with c2:
                st.write("**Bottom ημέρες**")
                st.dataframe(worst[["year", "month", "day", "floor", "price"]], use_container_width=True)
            explain("Γρήγορη ανάδειξη εξαιρετικών ή αδύναμων ημερών για έλεγχο/διορθωτικές ενέργειες.")
        else:
            st.info("Δεν υπάρχουν τιμές για Top/Bottom.")

        st.subheader("Ζώνες τιμολόγησης (price bands)")
        if fdf["price"].notna().any():
            bands_edges = st.multiselect(
                "Όρια ζωνών (π.χ. 50, 100, 150)",
                options=[25, 50, 75, 100, 125, 150, 200, 250, 300],
                default=[50, 100, 150],
                help="Ορίζει τα όρια σε € για τον υπολογισμό του ποσοστού κρατήσεων ανά ζώνη.",
            )
            edges = sorted(set([0] + [int(x) for x in bands_edges if x is not None] + [int(max(1000, fdf['price'].max()*1.1))]))
            # Κοψίματα
            labels = []
            for i in range(len(edges)-1):
                labels.append(f"{edges[i]}–{edges[i+1]}")
            cut = pd.cut(fdf["price"], bins=edges, labels=labels, include_lowest=True, right=True)
            # Επιβάλλουμε σειρά κατηγοριών για σωστή αύξουσα ταξινόμηση στον άξονα
            fdf.loc[:, "band"] = pd.Categorical(cut, categories=labels, ordered=True)
            band_tbl = fdf.groupby(["year", "band"]).size().reset_index(name="count")
            if not band_tbl.empty:
                st.vega_lite_chart(
                    band_tbl,
                    {
                        "mark": "bar",
                        "encoding": {
                            "x": {"field": "band", "type": "ordinal", "title": "Ζώνη τιμής", "sort": labels},
                            "y": {"field": "count", "type": "quantitative", "title": "Κρατήσεις"},
                            "color": {"field": "year", "type": "nominal", "title": "Έτος"},
                            "tooltip": [
                                {"field": "year", "type": "ordinal"},
                                {"field": "band", "type": "ordinal"},
                                {"field": "count", "type": "quantitative"}
                            ]
                        },
                        "width": "container",
                        "height": 240
                    },
                    use_container_width=True,
                )
                explain("Κατανομή κρατήσεων ανά εύρος τιμής — βοηθά να δεις πού κινείται η ζήτηση.")
            else:
                st.info("Δεν βρέθηκαν κρατήσεις για να υπολογιστούν ζώνες τιμών.")
        else:
            st.info("Δεν υπάρχουν τιμές για ζώνες.")

        st.subheader("Σύνοψη τρέχοντος μήνα (YoY)")
        # Επιλογή μήνα προς ανάλυση
        month_sel = st.selectbox("Μήνας", MONTHS, index=MONTHS.index("Αύγουστος") if "Αύγουστος" in MONTHS else 0, key="yoy_month_sel")
        curm = fdf[fdf["month"] == month_sel]
        if not curm.empty:
            ys = sorted(curm["year"].dropna().astype(int).unique())
            if len(ys) >= 2:
                # Επιλογή ζεύγους ετών προς σύγκριση (παραμετροποιήσιμο)
                y_idx_latest = len(ys) - 1
                y_idx_prev = len(ys) - 2
                colA, colB = st.columns(2)
                with colA:
                    y_now = st.selectbox("Έτος A", ys, index=y_idx_latest, key="yoy_year_now")
                with colB:
                    y_prev = st.selectbox("Έτος B (βάση)", ys, index=y_idx_prev, key="yoy_year_prev")
                if y_now == y_prev:
                    st.warning("Επίλεξε διαφορετικά έτη για σύγκριση.")
                else:
                    # Metrics για το επιλεγμένο ζεύγος ετών
                    rev_now = float(curm[curm["year"] == y_now]["price"].sum()) if curm["price"].notna().any() else 0.0
                    rev_prev = float(curm[curm["year"] == y_prev]["price"].sum()) if curm["price"].notna().any() else 0.0
                    occ_now = int(curm[curm["year"] == y_now]["day"].dropna().nunique())
                    occ_prev = int(curm[curm["year"] == y_prev]["day"].dropna().nunique())
                    med_now = float(curm[(curm["year"] == y_now) & curm["price"].notna()]["price"].median()) if curm["price"].notna().any() else 0.0
                    med_prev = float(curm[(curm["year"] == y_prev) & curm["price"].notna()]["price"].median()) if curm["price"].notna().any() else 0.0
                    m1, m2, m3 = st.columns(3)
                    with m1:
                        delta_rev = rev_now - rev_prev
                        st.metric(f"Έσοδα ({y_now} vs {y_prev})", f"{rev_now:,.0f}", delta=f"{delta_rev:+.0f}")
                    with m2:
                        delta_occ = occ_now - occ_prev
                        st.metric(f"Πληρότητα ημέρες ({y_now} vs {y_prev})", f"{occ_now}", delta=f"{delta_occ:+d}")
                    with m3:
                        if med_prev != 0:
                            pct = ((med_now - med_prev)/med_prev)*100
                            st.metric(f"Διάμεση τιμή ({y_now} vs {y_prev})", f"{med_now:.0f}", delta=f"{pct:+.1f}%")
                        else:
                            st.metric(f"Διάμεση τιμή ({y_now} vs {y_prev})", f"{med_now:.0f}")
                    explain("Σύντομη σύγκριση για τον επιλεγμένο μήνα ανάμεσα στα έτη που διάλεξες.")

                # Επιλογή να δούμε ΟΛΑ τα YoY ζεύγη για τον μήνα
                show_all = st.checkbox("Δείξε όλα τα YoY ζεύγη για τον μήνα", value=False, key="yoy_show_all_month")
                if show_all:
                    # Συγκεντρώνουμε ανά έτος τα KPIs και υπολογίζουμε Δ vs προηγούμενο έτος
                    tmp = []
                    for y in ys:
                        sub = curm[curm["year"] == y]
                        rev = float(sub["price"].sum()) if sub["price"].notna().any() else 0.0
                        occ = int(sub["day"].dropna().nunique())
                        med = float(sub[sub["price"].notna()]["price"].median()) if sub["price"].notna().any() else 0.0
                        tmp.append({"year": y, "revenue": rev, "occupancy_days": occ, "median_price": med})
                    yoy_tbl = pd.DataFrame(tmp).sort_values("year").reset_index(drop=True)
                    # Deltas vs previous
                    yoy_tbl["rev_delta"] = yoy_tbl["revenue"].diff()
                    yoy_tbl["occ_delta"] = yoy_tbl["occupancy_days"].diff()
                    yoy_tbl["median_delta_pct"] = yoy_tbl["median_price"].pct_change() * 100
                    st.write(f"**YoY πίνακας για {month_sel}**")
                    st.dataframe(yoy_tbl, use_container_width=True)
                    # Γράφημα διαφορών εσόδων
                    if len(yoy_tbl) >= 2:
                        st.vega_lite_chart(
                            yoy_tbl.assign(year_str=yoy_tbl["year"].astype(str)),
                            {
                                "mark": "bar",
                                "encoding": {
                                    "x": {"field": "year_str", "type": "ordinal", "title": "Έτος"},
                                    "y": {"field": "rev_delta", "type": "quantitative", "title": "Δ έσοδα vs προηγούμενο"},
                                    "tooltip": [
                                        {"field": "year", "type": "ordinal"},
                                        {"field": "rev_delta", "type": "quantitative", "title": "Δ έσοδα"},
                                        {"field": "occ_delta", "type": "quantitative", "title": "Δ ημέρες"},
                                        {"field": "median_delta_pct", "type": "quantitative", "title": "Δ διάμεση %"}
                                    ]
                                },
                                "width": "container",
                                "height": 220
                            },
                            use_container_width=True,
                        )
                        explain("Όλες οι ετήσιες μεταβολές για τον μήνα — έσοδα, ημέρες με κράτηση και διάμεση τιμή.")
            else:
                st.info("Χρειάζονται τουλάχιστον δύο έτη για YoY στον επιλεγμένο μήνα.")
        else:
            st.info("Δεν υπάρχουν δεδομένα για τον επιλεγμένο μήνα.")

        st.subheader("Ανώμαλες ημέρες (spikes/dips τιμής ανά μήνα)")
        # Z-score ανά (έτος, μήνα) πάνω στο price
        if fdf["price"].notna().any():
            g = fdf.dropna(subset=["price"]).groupby(["year", "month"]) ["price"]
            zdf = g.apply(lambda s: (s - s.mean())/s.std(ddof=0) if s.std(ddof=0) not in (0, np.nan) else pd.Series([0]*len(s), index=s.index)).reset_index(name="z")
            merged = fdf.join(zdf[["z"]], how="left")
            # Top |z| 10
            merged["absz"] = merged["z"].abs()
            outliers = merged.sort_values("absz", ascending=False).head(10)
            if not outliers.empty:
                st.write("**Top 10 ανωμαλίες** (|z| μεγαλύτερο)")
                st.dataframe(outliers[["year", "month", "day", "floor", "price", "z"]], use_container_width=True)
                explain("Ημέρες με ασυνήθιστα υψηλή/χαμηλή τιμή (z-score). Χρήσιμες για διερεύνηση σφαλμάτων ή ευκαιριών.")
            else:
                st.info("Δεν βρέθηκαν ανωμαλίες.")
        else:
            st.info("Δεν υπάρχουν τιμές για ανίχνευση ανωμαλιών.")

   
