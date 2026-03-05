# streamlit_app.py - Laboratory Reagent Inventory System
# Single-file version (no external modules)
# Full CRUD using Google Sheets API v4
# Multi-user hardening: soft-lock + audit log (optional sheets)
# Location dropdown + CustomEntry
# Scan & Add: Camera + Bulletproof Uploader (stores bytes in session_state)
# OCR: Cloud OCR-ready hooks + local parser; preview is Streamlit-version-safe
# Slack + Email alerts (optional)
# Revised: January 2026

import streamlit as st
import pandas as pd
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import urllib.request
import smtplib
from email.message import EmailMessage
import time
import re
import io
import base64

import requests

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Pillow optional (preview + optional local crop; OCR can be cloud)
PIL_AVAILABLE = True
try:
    from PIL import Image
except Exception:
    PIL_AVAILABLE = False
    Image = None

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
WORKSHEET_INVENTORY = "template"   # inventory tab name
WORKSHEET_LOCKS = "locks"         # optional locks tab (recommended)
WORKSHEET_AUDIT = "audit_log"     # optional audit tab (recommended)
WORKSHEET_USAGE = "usage_log"


SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

EXPECTED_COLS = [
    "id", "name", "cas_number", "supplier", "location",
    "quantity", "unit", "expiration_date", "low_stock_threshold"
]

LOCATION_CHOICES = [
    "Scappy-Doo (-30c)",
    "Daphne (-30c)",
    "Tom (-80c)",
    "Jerry (-80c)",
    "Sammy (-80c)",
    "Scooby-Doo (-30c)",
    "Velma (4c)",
    "CustomEntry",
]

READ_RANGE = f"{WORKSHEET_INVENTORY}!A1:Z5000"

# ─────────────────────────────────────────────────────────────────────────────
# APP UI
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="LabTrack", page_icon="🧪", layout="wide")

# ─────────────────────────────────────────────────────────────────────────────
# UI THEME (CSS)
# ─────────────────────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
      /* Layout */
      .block-container {padding-top: 1.2rem; padding-bottom: 2rem;}
      /* Sidebar */
      [data-testid="stSidebar"] {background: #0b1630;}
      [data-testid="stSidebar"] * {color: #e8eefc;}
      .lt-logo {display:flex; align-items:center; gap:.6rem; padding:.25rem .25rem .75rem .25rem;}
      .lt-logo-badge {width:36px; height:36px; border-radius:10px; background:#2b6cff; display:flex; align-items:center; justify-content:center; font-weight:700;}
      .lt-logo-title {font-size:1.15rem; font-weight:700; line-height:1;}
      .lt-logo-sub {font-size:.75rem; opacity:.8; margin-top:.1rem;}
      /* Pills */
      .lt-pill {display:inline-flex; align-items:center; gap:.5rem; padding:.35rem .65rem; border-radius:999px; font-weight:700; font-size:.8rem; border:1px solid rgba(0,0,0,.06);}
      .lt-pill span.badge {display:inline-flex; align-items:center; justify-content:center; min-width:28px; height:18px; padding:0 .4rem; border-radius:999px; font-size:.75rem; font-weight:800;}
      .pill-warn {background:#fff4e5; color:#a15800;}
      .pill-warn span.badge {background:#ffe0b2; color:#7a4100;}
      .pill-danger {background:#ffecec; color:#b00020;}
      .pill-danger span.badge {background:#ffcdd2; color:#7f0015;}
      /* Cards */
      .lt-card {background:#ffffff; border:1px solid rgba(0,0,0,.06); border-radius:14px; padding:1rem 1.1rem; box-shadow:0 2px 10px rgba(0,0,0,.04);}
      .lt-card .k {font-size:.75rem; font-weight:800; opacity:.65; letter-spacing:.03em;}
      .lt-card .v {font-size:2.1rem; font-weight:900; margin-top:.25rem;}
      .lt-card .s {font-size:.85rem; opacity:.6; margin-top:.1rem;}
      .lt-border-blue {border-top:4px solid #3b82f6;}
      .lt-border-orange {border-top:4px solid #f59e0b;}
      .lt-border-red {border-top:4px solid #ef4444;}
      .lt-border-green {border-top:4px solid #22c55e;}
      /* Alert list items */
      .lt-alert {background:#fff7e6; border:1px solid rgba(245,158,11,.35); border-left:5px solid #f59e0b;
                border-radius:12px; padding:.85rem 1rem; display:flex; align-items:center; justify-content:space-between; gap:1rem;}
      .lt-alert .t {font-weight:900;}
      .lt-alert .m {font-size:.85rem; opacity:.75; margin-top:.2rem;}
      .lt-tag {font-size:.75rem; font-weight:900; padding:.25rem .55rem; border-radius:999px; background:#ffedd5; color:#9a3412; border:1px solid rgba(154,52,18,.2);}
      /* Topbar */
      .lt-topbar {display:flex; align-items:center; justify-content:space-between; margin-bottom:1rem;}
      .lt-crumb {font-weight:900; letter-spacing:.02em; color:#6b7280;}
      .lt-crumb b {color:#111827;}
      .lt-status {display:flex; align-items:center; gap:.55rem;}
      .lt-dot {width:10px; height:10px; border-radius:50%; background:#22c55e; display:inline-block;}
    </style>
    """,
    unsafe_allow_html=True,
)

# ─────────────────────────────────────────────────────────────────────────────
# STREAMLIT VERSION SAFE UI HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def st_image_safe(img, caption=None):
    """Works on old and new Streamlit (use_container_width vs use_column_width)."""
    try:
        st.image(img, caption=caption, use_container_width=True)
    except TypeError:
        st.image(img, caption=caption, use_column_width=True)

def st_dataframe_safe(df, **kwargs):
    """Works on old and new Streamlit (use_container_width vs use_column_width)."""
    try:
        st.dataframe(df, use_container_width=True, **kwargs)
    except TypeError:
        st.dataframe(df, use_column_width=True, **kwargs)

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def now_utc():
    return datetime.now(timezone.utc)

def with_retries(fn, tries=4, base_sleep=0.5):
    last = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:
            last = e
            time.sleep(base_sleep * (2 ** i))
    raise last

def safe_str(x) -> str:
    return "" if x is None else str(x)

def colnum_to_a1(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def build_header_map(headers: list[str]) -> dict[str, int]:
    return {h: i + 1 for i, h in enumerate(headers) if str(h).strip()}

def NA(v) -> str:
    v = "" if v is None else str(v).strip()
    return v if v else "N/A"

def location_widget(label: str, value: str, key: str) -> str:
    default_idx = LOCATION_CHOICES.index(value) if value in LOCATION_CHOICES else LOCATION_CHOICES.index("CustomEntry")
    choice = st.selectbox(label, LOCATION_CHOICES, index=default_idx, key=f"{key}_choice")
    if choice == "CustomEntry":
        custom = st.text_input("Custom location", value=value if value not in LOCATION_CHOICES else "", key=f"{key}_custom")
        return custom.strip()
    return choice

def append_usage_log(ts_iso, user, role, action, name, cas, amount_used, unit, qty_before, qty_after, location, notes):
    # If sheet doesn't exist, just skip logging (doesn't break deduction)
    try:
        _ = get_sheet_id_by_title(WORKSHEET_USAGE)
    except Exception:
        return

    row = [
        ts_iso,
        user,
        role,
        action,
        name,
        cas,
        str(amount_used),
        unit,
        str(qty_before),
        str(qty_after),
        location,
        notes or ""
    ]

    with_retries(lambda: sheets.values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{WORKSHEET_USAGE}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [row]},
    ).execute())
#import from a CSV file    
def normalize_col(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(s).strip().lower()).strip("_")

def csv_to_inventory_rows(df_csv: pd.DataFrame, raw_headers: list[str]) -> list[list[str]]:
    """
    Convert CSV dataframe to a list of rows matching the Google Sheet header order (RAW_HEADERS).
    Missing values -> "N/A".
    """
    if df_csv is None or df_csv.empty:
        return []

    df = df_csv.copy()
    df.columns = [normalize_col(c) for c in df.columns]

    # Common aliases in CSV files -> your sheet columns
    alias = {
        "id": ["id", "reagent_id", "item_id"],
        "name": ["name", "reagent", "reagent_name", "chemical", "chemical_name", "product_name"],
        "cas_number": ["cas", "cas_number", "cas_no", "cas#", "casnum"],
        "supplier": ["supplier", "vendor", "manufacturer", "company", "brand"],
        "location": ["location", "storage", "freezer", "shelf", "position"],
        "quantity": ["quantity", "qty", "amount", "volume", "mass", "stock"],
        "unit": ["unit", "units", "uom"],
        "expiration_date": ["expiration_date", "expiry", "expiry_date", "exp", "exp_date", "expiration"],
        "low_stock_threshold": ["low_stock_threshold", "threshold", "low_stock", "min_stock", "reorder_level"],
    }

    # Build reverse lookup: normalized CSV column -> canonical field
    col_map = {}
    for canonical, candidates in alias.items():
        for c in candidates:
            c_norm = normalize_col(c)
            if c_norm in df.columns:
                col_map[canonical] = c_norm
                break  # first match wins

    # Ensure required minimal columns exist
    if "name" not in col_map:
        raise ValueError("CSV must include a 'name' column (or alias like reagent_name/chemical).")

    # Create rows in sheet header order
    rows_out = []
    for _, r in df.iterrows():
        record = {}

        # Pull values from CSV (or N/A)
        for canonical in alias.keys():
            if canonical in col_map:
                val = r.get(col_map[canonical])
                if pd.isna(val) or str(val).strip() == "":
                    record[canonical] = "N/A"
                else:
                    record[canonical] = str(val).strip()
            else:
                # Not provided in CSV
                record[canonical] = "N/A"

        # Normalize numeric fields
        # quantity
        try:
            if record["quantity"] != "N/A":
                record["quantity"] = str(float(record["quantity"]))
        except:
            record["quantity"] = "N/A"

        # low_stock_threshold
        try:
            if record["low_stock_threshold"] != "N/A":
                record["low_stock_threshold"] = str(float(record["low_stock_threshold"]))
        except:
            record["low_stock_threshold"] = "N/A"

        # expiration_date -> ISO if parseable, else N/A
        if record["expiration_date"] != "N/A":
            dt = pd.to_datetime(record["expiration_date"], errors="coerce")
            record["expiration_date"] = dt.date().isoformat() if pd.notnull(dt) else "N/A"

        # Build a sheet row in RAW_HEADERS order
        sheet_row = []
        for h in raw_headers:
            h_norm = normalize_col(h)
            # match against your expected cols
            if h_norm in [normalize_col(x) for x in EXPECTED_COLS]:
                # map header norm back to canonical key
                # easiest: use EXPECTED_COLS list order
                # find canonical key whose normalize_col matches h_norm
                canonical_key = None
                for k in EXPECTED_COLS:
                    if normalize_col(k) == h_norm:
                        canonical_key = k
                        break
                sheet_row.append(record.get(canonical_key, "N/A"))
            else:
                # sheet has extra columns not in our schema -> blank
                sheet_row.append("")
        rows_out.append(sheet_row)

    return rows_out

def append_rows_bulk(rows: list[list[str]]):
    """Append many rows to the inventory sheet in one API call."""
    if not rows:
        return
    with_retries(lambda: sheets.values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{WORKSHEET_INVENTORY}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute())


# ─────────────────────────────────────────────────────────────────────────────
# CLOUD OCR (Google Vision) - optional usage
# ─────────────────────────────────────────────────────────────────────────────
def vision_available() -> bool:
    
    return bool(st.secrets.get("gcp_vision", {}).get("api_key", ""))

# def vision_text_detection(image_bytes: bytes) -> dict:
#     """Google Vision TEXT_DETECTION using API key in secrets: [gcp_vision] api_key=..."""
    
#     api_key = st.secrets["gcp_vision"]["api_key"]
#     b64 = base64.b64encode(image_bytes).decode("utf-8")
#     url = f"https://vision.googleapis.com/v1/images:annotate?key={api_key}"
#     payload = {
#         "requests": [{
#             "image": {"content": b64},
#             "features": [{"type": "TEXT_DETECTION"}]
            
#         }]
#     }

#############debug##########
def vision_text_detection(image_bytes: bytes) -> dict:
    api_key = st.secrets["gcp_vision"]["api_key"]
    b64 = base64.b64encode(image_bytes).decode("utf-8")
    url = f"https://vision.googleapis.com/v1/images:annotate?key={api_key}"

    payload = {
        "requests": [{
            "image": {"content": b64},
            "features": [{"type": "TEXT_DETECTION"}],
        }]
    }

    r = requests.post(url, json=payload, timeout=30)

    if not r.ok:
        st.error(f"Vision OCR failed: HTTP {r.status_code}")
        try:
            st.code(r.text, language="json")
        except Exception:
            st.write(r.text)
        r.raise_for_status()

    data = r.json()

    err = data.get("responses", [{}])[0].get("error")
    if err:
        st.error("Vision OCR error returned by API")
        st.code(json.dumps(err, indent=2), language="json")
        raise RuntimeError(err.get("message", "Vision OCR error"))

    return data

    
    

def vision_extract_full_text(resp_json: dict) -> str:
    try:
        ann = resp_json["responses"][0].get("textAnnotations", [])
        return ann[0]["description"] if ann else ""
    except Exception:
        return ""

def vision_compute_text_bbox(resp_json: dict):
    """
    Compute bbox around detected word boxes (excludes full-page annotation).
    Returns (min_x, min_y, max_x, max_y) or None.
    """
    try:
        ann = resp_json["responses"][0].get("textAnnotations", [])
        if len(ann) <= 1:
            return None
        xs, ys = [], []
        for a in ann[1:]:
            poly = a.get("boundingPoly", {}).get("vertices", [])
            for v in poly:
                if "x" in v and "y" in v:
                    xs.append(int(v["x"]))
                    ys.append(int(v["y"]))
        if not xs or not ys:
            return None
        return (min(xs), min(ys), max(xs), max(ys))
    except Exception:
        return None

def autocrop_bytes_using_vision(image_bytes: bytes, margin_ratio: float = 0.06) -> bytes:
    """
    Auto-crop based on Vision bbox. Requires Pillow for actual crop.
    If Pillow not available or bbox not found, returns original bytes.
    """
    if not (PIL_AVAILABLE and vision_available()):
        return image_bytes

    resp = vision_text_detection(image_bytes)
    bbox = vision_compute_text_bbox(resp)
    if not bbox:
        return image_bytes

    try:
        img = Image.open(io.BytesIO(image_bytes))
        w, h = img.size
        x0, y0, x1, y1 = bbox

        mx = int(w * margin_ratio)
        my = int(h * margin_ratio)

        x0 = max(0, x0 - mx)
        y0 = max(0, y0 - my)
        x1 = min(w, x1 + mx)
        y1 = min(h, y1 + my)

        if x1 - x0 < 40 or y1 - y0 < 40:
            return image_bytes

        cropped = img.crop((x0, y0, x1, y1))
        out = io.BytesIO()
        cropped.save(out, format="PNG")
        return out.getvalue()
    except Exception:
        return image_bytes

# ─────────────────────────────────────────────────────────────────────────────
# PARSING (CAS, supplier, expiration; missing => N/A)
# ─────────────────────────────────────────────────────────────────────────────
def _find_first(patterns, text, flags=re.IGNORECASE):
    for p in patterns:
        m = re.search(p, text, flags)
        if m:
            return m.group(1).strip()
    return None

def parse_reagent_fields(text: str) -> dict:
    """Best-effort parser; fills missing values with N/A. Uses cas_number only."""
    t = " ".join(text.split())

    cas = _find_first(
        [
            r"CAS[:\s]*([0-9]{2,7}-[0-9]{2}-[0-9])",
            r"CAS\s*No\.?[:\s]*([0-9]{2,7}-[0-9]{2}-[0-9])",
        ],
        t
    )

    supplier = _find_first(
        [
            r"(?:Supplier|Manufacturer|Mfr\.?)[:\s]*([A-Za-z0-9 &\-\.,]+)",
        ],
        t
    )

    exp_raw = _find_first(
        [
            r"(?:Exp(?:iration)?|Expiry|Use\s*By|Best\s*Before)[:\s]*([0-9]{4}[-/][0-9]{1,2}[-/][0-9]{1,2})",
            r"(?:Exp(?:iration)?|Expiry|Use\s*By|Best\s*Before)[:\s]*([0-9]{1,2}[-/][0-9]{1,2}[-/][0-9]{2,4})",
        ],
        t
    )

    exp_iso = ""
    if exp_raw:
        dt = pd.to_datetime(exp_raw, errors="coerce")
        if pd.notnull(dt):
            exp_iso = dt.date().isoformat()

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    name_guess = "N/A"
    for ln in lines[:10]:
        if len(ln) >= 4 and not re.fullmatch(r"[A-Z0-9\-_]+", ln):
            name_guess = ln
            break

    return {
        "id": "N/A",
        "name": NA(name_guess),
        "cas_number": NA(cas),
        "supplier": NA(supplier),
        "location": "N/A",
        "quantity": "0",
        "unit": "N/A",
        "expiration_date": exp_iso if exp_iso else "N/A",
    }

# ─────────────────────────────────────────────────────────────────────────────
# NOTIFY (Slack + Email) — optional
# ─────────────────────────────────────────────────────────────────────────────
def send_slack(text: str):
    url = st.secrets.get("slack", {}).get("webhook_url", "")
    if not url:
        return
    payload = {"text": text}
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    urllib.request.urlopen(req, timeout=10)

def send_email(subject: str, body: str):
    cfg = st.secrets.get("email", {})
    host = cfg.get("smtp_host")
    port = int(cfg.get("smtp_port", 587))
    user = cfg.get("smtp_user")
    pwd  = cfg.get("smtp_pass")
    to   = cfg.get("to")
    if not (host and user and pwd and to):
        return

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = to
    msg.set_content(body)

    with smtplib.SMTP(host, port, timeout=15) as s:
        s.starttls()
        s.login(user, pwd)
        s.send_message(msg)

# ─────────────────────────────────────────────────────────────────────────────
# GOOGLE SHEETS INIT
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_resource(show_spinner="Connecting to Google Sheets...")
def get_sheets_service_and_id():
    sa_info = st.secrets["google_service_account"]
    creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    svc = build("sheets", "v4", credentials=creds).spreadsheets()
    spreadsheet_id = st.secrets["connections"]["gsheets"]["spreadsheet"]
    return svc, spreadsheet_id

sheets, SPREADSHEET_ID = get_sheets_service_and_id()

def get_sheet_id_by_title(sheet_title: str) -> int:
    meta = with_retries(lambda: sheets.get(spreadsheetId=SPREADSHEET_ID).execute())
    for s in meta.get("sheets", []):
        props = s.get("properties", {})
        if props.get("title") == sheet_title:
            return int(props.get("sheetId"))
    raise ValueError(f"Sheet tab '{sheet_title}' not found. Check WORKSHEET_* constants.")

def sheet_exists(sheet_title: str) -> bool:
    try:
        get_sheet_id_by_title(sheet_title)
        return True
    except Exception:
        return False

LOCKS_ENABLED = sheet_exists(WORKSHEET_LOCKS)
AUDIT_ENABLED = sheet_exists(WORKSHEET_AUDIT)

# ─────────────────────────────────────────────────────────────────────────────
# AUTH (demo)
# ─────────────────────────────────────────────────────────────────────────────
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
    st.session_state.username = None
    st.session_state.role = None

if not st.session_state.authenticated:
    st.subheader("🔐 Login Required")
    with st.form("login_form", clear_on_submit=True):
        c1, c2 = st.columns([3, 2])
        with c1:
            username = st.text_input("Username")
        with c2:
            password = st.text_input("Password", type="password")

        if st.form_submit_button("Login", type="primary", use_container_width=True):
            hashed = hashlib.sha256(password.encode()).hexdigest()
            if username == "admin" and hashed == hashlib.sha256("admin123".encode()).hexdigest():
                st.session_state.update(authenticated=True, username=username, role="admin")
            elif username == "user" and hashed == hashlib.sha256("user123".encode()).hexdigest():
                st.session_state.update(authenticated=True, username=username, role="user")

            if st.session_state.authenticated:
                st.success(f"Welcome, {username}! ({st.session_state.role})")
                st.rerun()
            else:
                st.error("Invalid username or password")
    st.stop()

# Sidebar session controls are handled in sidebar_nav()

# ─────────────────────────────────────────────────────────────────────────────
# LOCKS (soft-lock)
# ─────────────────────────────────────────────────────────────────────────────
def try_lock_row(rownum: int, user: str, purpose: str, lease_seconds=90) -> bool:
    if not LOCKS_ENABLED:
        return True

    rng = f"{WORKSHEET_LOCKS}!A1:D5000"
    res = with_retries(lambda: sheets.values().get(spreadsheetId=SPREADSHEET_ID, range=rng).execute())
    vals = res.get("values", [])
    locks = vals[1:] if len(vals) > 1 else []

    now = now_utc()
    until = now + timedelta(seconds=lease_seconds)
    until_iso = until.isoformat()

    for r in locks:
        if len(r) < 3:
            continue
        try:
            locked_row = int(r[0])
        except:
            continue
        if locked_row != rownum:
            continue

        locked_by = r[1] if len(r) > 1 else ""
        locked_until = r[2] if len(r) > 2 else ""
        try:
            locked_until_dt = datetime.fromisoformat(locked_until)
        except:
            locked_until_dt = now - timedelta(days=365)

        if locked_until_dt > now and locked_by and locked_by != user:
            return False

    with_retries(lambda: sheets.values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{WORKSHEET_LOCKS}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [[str(rownum), user, until_iso, purpose]]},
    ).execute())
    return True

# ─────────────────────────────────────────────────────────────────────────────
# AUDIT LOG
# ─────────────────────────────────────────────────────────────────────────────
def audit(action: str, row: int, reagent_name: str, details: str):
    if not AUDIT_ENABLED:
        return
    ts = now_utc().isoformat()
    with_retries(lambda: sheets.values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{WORKSHEET_AUDIT}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [[ts, st.session_state.username, st.session_state.role, action, str(row), reagent_name, details]]},
    ).execute())


def parse_expiration_dates(series: pd.Series) -> pd.Series:
    """Parse expiration dates robustly.

    Handles:
      - empty / N/A values
      - multiple date string formats (month-first and day-first)
      - Excel serial date numbers (e.g., 45234)
    Returns a Series of python datetime.date (or NaT).
    """
    if series is None:
        return pd.Series(dtype="object")

    s = series.copy()

    # Normalize empties
    # Keep non-strings as-is; for strings, strip whitespace.
    def _clean(v):
        if v is None:
            return None
        if isinstance(v, str):
            vv = v.strip()
            if vv == "" or vv.lower() in {"n/a", "na", "none", "null"}:
                return None
            return vv
        return v

    s = s.map(_clean)

    # Excel serial numbers (commonly show up when a date cell is read as a number)
    num = pd.to_numeric(s, errors="coerce")
    serial_mask = num.notna() & num.between(20000, 80000)  # ~1954-2119

    parsed = pd.to_datetime(s, errors="coerce")

    # Try day-first parsing for leftovers (common for non-US entry)
    leftover = parsed.isna() & s.notna()
    if leftover.any():
        parsed_dayfirst = pd.to_datetime(s[leftover], errors="coerce", dayfirst=True)
        parsed.loc[leftover] = parsed_dayfirst

    # Apply serial parsing last (so numeric strings like 20260301 aren't treated as serials)
    if serial_mask.any():
        parsed_serial = pd.to_datetime(num[serial_mask], unit="D", origin="1899-12-30", errors="coerce")
        parsed.loc[serial_mask] = parsed_serial

    return parsed.dt.date


# ─────────────────────────────────────────────────────────────────────────────
# LOAD INVENTORY
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=120, show_spinner="Loading inventory...")
def load_inventory():
    result = with_retries(lambda: sheets.values().get(spreadsheetId=SPREADSHEET_ID, range=READ_RANGE).execute())
    values = result.get("values", [])
    if not values:
        return pd.DataFrame(), {}, []

    headers = [safe_str(x).strip() for x in values[0]]
    header_map = build_header_map(headers)

    data = values[1:]
    norm = []
    for row in data:
        row = list(row)
        if len(row) < len(headers):
            row += [""] * (len(headers) - len(row))
        norm.append(row[:len(headers)])

    df = pd.DataFrame(norm, columns=headers)

    for c in EXPECTED_COLS:
        if c not in df.columns:
            df[c] = ""

    df["_row"] = [i + 2 for i in range(len(df))]
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0.0)
    df["low_stock_threshold"] = pd.to_numeric(df["low_stock_threshold"], errors="coerce").fillna(10.0)
    df["expiration_date"] = parse_expiration_dates(df["expiration_date"])

    df = df[EXPECTED_COLS + ["_row"]]
    return df, header_map, headers

reagents_df, HEADER_MAP, RAW_HEADERS = load_inventory()

# ─────────────────────────────────────────────────────────────────────────────
# CRUD OPS
# ─────────────────────────────────────────────────────────────────────────────
def update_row_cells(rownum: int, updates: dict):
    for col, val in updates.items():
        if col not in HEADER_MAP:
            continue
        letter = colnum_to_a1(HEADER_MAP[col])
        a1 = f"{WORKSHEET_INVENTORY}!{letter}{rownum}"
        with_retries(lambda: sheets.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=a1,
            valueInputOption="RAW",
            body={"values": [[val]]},
        ).execute())

def append_row(values_by_col: dict):
    if not RAW_HEADERS:
        raise ValueError("Sheet header row missing in inventory sheet.")
    row = []
    for h in RAW_HEADERS:
        row.append(values_by_col.get(h, "") if h in values_by_col else "")
    with_retries(lambda: sheets.values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{WORKSHEET_INVENTORY}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [row]},
    ).execute())

def delete_row(rownum: int):
    sheet_id = get_sheet_id_by_title(WORKSHEET_INVENTORY)
    with_retries(lambda: sheets.batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"requests": [{"deleteDimension": {"range": {
            "sheetId": sheet_id,
            "dimension": "ROWS",
            "startIndex": rownum - 1,
            "endIndex": rownum
        }}}]}
    ).execute())

# ─────────────────────────────────────────────────────────────────────────────
# ALERTS (NaT-safe)
# ─────────────────────────────────────────────────────────────────────────────
def build_alerts(df: pd.DataFrame):
    low, expired = [], []
    today = date.today()
    if df.empty:
        return low, expired

    for _, row in df.iterrows():
        name = str(row.get("name", "") or "?")
        unit = str(row.get("unit", "") or "?")
        qty = float(row.get("quantity", 0) or 0)
        thresh = float(row.get("low_stock_threshold", 10) or 10)

        if qty <= thresh:
            low.append(f"{name}: {qty:.2f} {unit} (threshold {thresh:.2f})")

        exp = row.get("expiration_date")
        if pd.notnull(exp):
            exp_date = exp.date() if hasattr(exp, "date") else exp
            if exp_date < today:
                expired.append(f"{name}: expired {exp_date}")

    return low, expired

low_alerts, expired_alerts = build_alerts(reagents_df)

# Store alert text (render it on the Dashboard page for a cleaner UI)
st.session_state["alert_banner_md"] = ""
if low_alerts or expired_alerts:
    parts = []
    if low_alerts:
        parts.append("⚠️ **Low stock**\n" + "\n".join([f"- {x}" for x in low_alerts]))
    if expired_alerts:
        parts.append("❌ **Expired**\n" + "\n".join([f"- {x}" for x in expired_alerts]))
    st.session_state["alert_banner_md"] = "\n\n".join(parts)

# ─────────────────────────────────────────────────────────────────────────────
# SCAN IMAGE STORAGE (prevents uploader/camera from disappearing on rerun)
# ─────────────────────────────────────────────────────────────────────────────
def set_scan_image_bytes(b: bytes, source: str, meta: dict):
    st.session_state["scan_image_bytes"] = b
    st.session_state["scan_image_source"] = source
    st.session_state["scan_image_meta"] = meta

def clear_scan_image_bytes():
    st.session_state.pop("scan_image_bytes", None)
    st.session_state.pop("scan_image_source", None)
    st.session_state.pop("scan_image_meta", None)

# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# NAVIGATION + PAGES (LabTrack-style)
# ─────────────────────────────────────────────────────────────────────────────

def compute_dashboard_stats(df: pd.DataFrame):
    if df is None or df.empty:
        return {
            "total": 0,
            "low_stock": 0,
            "expiring_30": 0,
            "expired": 0,
            "healthy": 0,
        }

    today = date.today()
    expiring_cutoff = today + timedelta(days=30)

    low_mask = df["quantity"] <= df["low_stock_threshold"]
    exp_mask = df["expiration_date"].notna() & (df["expiration_date"] <= expiring_cutoff) & (df["expiration_date"] >= today)
    expired_mask = df["expiration_date"].notna() & (df["expiration_date"] < today)

    total = int(len(df))
    low_stock = int(low_mask.sum())
    expiring_30 = int(exp_mask.sum())
    expired = int(expired_mask.sum())

    healthy_mask = ~(low_mask | exp_mask | expired_mask)
    healthy = int(healthy_mask.sum())

    return {
        "total": total,
        "low_stock": low_stock,
        "expiring_30": expiring_30,
        "expired": expired,
        "healthy": healthy,
    }

def render_topbar(active_page: str):
    c1, c2 = st.columns([7, 3])
    with c1:
        st.markdown(
            f'<div class="lt-topbar"><div class="lt-crumb">LABTRACK / <b>{active_page.upper()}</b></div></div>',
            unsafe_allow_html=True
        )
    with c2:
        # Right aligned status + sync
        cc1, cc2 = st.columns([2, 1])
        with cc1:
            st.markdown(
                '<div class="lt-status"><span class="lt-dot"></span><span style="font-weight:800; color:#6b7280;">CONNECTED</span></div>',
                unsafe_allow_html=True
            )
        with cc2:
            if st.button("🔄 Sync", use_container_width=True, help="Reload inventory from Google Sheets", key="topbar_sync_btn"):
                load_inventory.clear()
                st.rerun()

def sidebar_nav():
    with st.sidebar:
        st.markdown(
            """
            <div class="lt-logo">
              <div class="lt-logo-badge">🧪</div>
              <div>
                <div class="lt-logo-title">LabTrack</div>
                <div class="lt-logo-sub">REAGENT INVENTORY</div>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.caption("OVERVIEW")
        page = st.radio(
            "Navigation",
            options=[
                "Dashboard",
                "All Reagents",
                "Inbound",
                "Barcode Scan",
                "Inventory Check",
                "Admin",
            ],
            index=0,
            label_visibility="collapsed",
        )

        st.divider()
        st.caption("SESSION")
        st.write(f"👤 **{st.session_state.username}** ({st.session_state.role})")

        if st.button("🚪 Logout", use_container_width=True, key="sidebar_logout_btn"):
            for k in ["authenticated", "username", "role"]:
                st.session_state.pop(k, None)
            st.rerun()

    return page

def render_dashboard(df: pd.DataFrame):
    render_topbar("Dashboard")
    stats = compute_dashboard_stats(df)

    banner = st.session_state.get('alert_banner_md','')
    if banner:
        st.warning(banner, icon='🚨')


    # Pills row
    st.markdown(
        f"""
        <div style="display:flex; gap:.75rem; flex-wrap:wrap; margin:.25rem 0 1rem 0;">
          <div class="lt-pill pill-warn">⚠️ LOW STOCK <span class="badge">{stats['low_stock']}</span></div>
          <div class="lt-pill pill-danger">⏱️ EXPIRING SOON <span class="badge">{stats['expiring_30']}</span></div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # KPI cards
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(f'<div class="lt-card lt-border-blue"><div class="k">TOTAL REAGENTS</div><div class="v">{stats["total"]}</div><div class="s">Items in inventory</div></div>', unsafe_allow_html=True)
    with c2:
        st.markdown(f'<div class="lt-card lt-border-orange"><div class="k">LOW STOCK</div><div class="v">{stats["low_stock"]}</div><div class="s">Below threshold</div></div>', unsafe_allow_html=True)
    with c3:
        st.markdown(f'<div class="lt-card lt-border-red"><div class="k">EXPIRING ≤30D</div><div class="v">{stats["expiring_30"]}</div><div class="s">Requires attention</div></div>', unsafe_allow_html=True)
    with c4:
        st.markdown(f'<div class="lt-card lt-border-green"><div class="k">HEALTHY</div><div class="v">{stats["healthy"]}</div><div class="s">In good standing</div></div>', unsafe_allow_html=True)

    st.markdown("<div style='height:.85rem'></div>", unsafe_allow_html=True)

    st.markdown("### Active Alerts")
    st.caption("Items requiring your attention")

    if df is None or df.empty:
        st.info("No inventory loaded.")
        return

    low_df = df[df["quantity"] <= df["low_stock_threshold"]].copy()
    if low_df.empty:
        st.success("No low-stock items 🎉")
        return

    # Show the most critical first (lowest % of threshold)
    low_df["pct"] = low_df.apply(lambda r: (r["quantity"] / r["low_stock_threshold"]) if r["low_stock_threshold"] else 0, axis=1)
    low_df = low_df.sort_values(["pct", "quantity"], ascending=[True, True]).head(12)

    for _, r in low_df.iterrows():
        name = str(r.get("name", "N/A"))
        qty = float(r.get("quantity", 0) or 0)
        thr = float(r.get("low_stock_threshold", 0) or 0)
        unit = str(r.get("unit", ""))
        supplier = str(r.get("supplier", "N/A"))
        location = str(r.get("location", "N/A"))
        st.markdown(
            f"""
            <div class="lt-alert">
              <div>
                <div class="t">{name}</div>
                <div class="m">STOCK: {qty:.2f}{(" "+unit) if unit else ""} · THR: {thr:.2f}{(" "+unit) if unit else ""} · {supplier} · {location}</div>
              </div>
              <div class="lt-tag">LOW STOCK</div>
            </div>
            <div style="height:.5rem"></div>
            """,
            unsafe_allow_html=True,
        )

def render_all_reagents(df: pd.DataFrame):
    render_topbar("All Reagents")

    st.markdown("### Reagents")
    search = st.text_input("Search", "")

    df_view = df.copy()
    if search and not df_view.empty:
        search_df = df_view.drop(columns=["_row"], errors="ignore").astype(str)
        mask = search_df.apply(lambda x: x.str.contains(search, case=False, na=False)).any(axis=1)
        df_view = df_view[mask].reset_index(drop=True)

    if df_view.empty:
        st.info("No matching reagents.")
        return

    st_dataframe_safe(df_view.drop(columns=["_row"], errors="ignore"), hide_index=True)

    st.markdown("### Row actions (Edit / Delete)")
    for _, r in df_view.iterrows():
        rownum = int(r.get("_row", 0))
        name = str(r.get("name", "") or "(no name)")

        with st.expander(f"{name} • sheet row {rownum}", expanded=False):
            cols = st.columns([3, 1])

            with cols[0]:
                with st.form(f"edit_{rownum}"):
                    name2 = st.text_input("Name", value=str(r.get("name", "")))
                    cas2 = st.text_input("CAS Number", value=str(r.get("cas_number", "")))
                    sup2 = st.text_input("Supplier", value=str(r.get("supplier", "")))
                    loc2 = location_widget("Location", value=str(r.get("location", "")), key=f"loc_{rownum}")
                    qty2 = st.number_input("Quantity", min_value=0.0, value=float(r.get("quantity", 0.0) or 0.0), step=0.1)
                    unit2 = st.text_input("Unit", value=str(r.get("unit", "")))

                    exp_val = r.get("expiration_date")
                    has_exp = st.checkbox("Has expiration date", value=pd.notnull(exp_val), key=f"hasexp_{rownum}")
                    exp2 = st.date_input(
                        "Expiration date",
                        value=(exp_val if pd.notnull(exp_val) else date.today()),
                        key=f"exp_{rownum}"
                    ) if has_exp else None

                    low2 = st.number_input("Low stock threshold", min_value=0.0, value=float(r.get("low_stock_threshold", 10.0) or 10.0), step=1.0)

                    if st.form_submit_button("💾 Save row", type="primary"):
                        if not try_lock_row(rownum, st.session_state.username, "edit"):
                            st.error("This row is being edited by someone else. Try again in ~1 minute.")
                            st.stop()

                        updates = {
                            "name": NA(name2),
                            "cas_number": NA(cas2),
                            "supplier": NA(sup2),
                            "location": NA(loc2),
                            "quantity": str(qty2),
                            "unit": NA(unit2),
                            "expiration_date": (exp2.isoformat() if exp2 else "N/A"),
                            "low_stock_threshold": str(low2),
                        }
                        update_row_cells(rownum, updates)
                        audit("UPDATE", rownum, NA(name2), json.dumps(updates, ensure_ascii=False))
                        st.success("Row updated.")
                        load_inventory.clear()
                        st.rerun()

            with cols[1]:
                if st.session_state.role != "admin":
                    st.info("Delete: admin only")
                else:
                    confirm = st.checkbox("Confirm delete", key=f"confirm_del_{rownum}")
                    if st.button("🗑️ Delete row", key=f"del_{rownum}", disabled=not confirm):
                        if not try_lock_row(rownum, st.session_state.username, "delete"):
                            st.error("This row is being edited by someone else. Try again in ~1 minute.")
                            st.stop()

                        delete_row(rownum)
                        audit("DELETE", rownum, name, "deleted row")
                        st.success("Row deleted.")
                        load_inventory.clear()
                        st.rerun()

def render_inbound_add():
    render_topbar("Inbound")

    st.markdown("### Add New Reagent")
    with st.form("add_form"):
        c1, c2, c3 = st.columns(3)
        with c1:
            new_id = st.text_input("ID", "")
            new_name = st.text_input("Name", "")
            new_cas = st.text_input("CAS Number", "")
        with c2:
            new_supplier = st.text_input("Supplier", "")
            new_location = location_widget("Location", value="", key="add_loc")
            new_unit = st.text_input("Unit", "mL")
        with c3:
            new_qty = st.number_input("Quantity", min_value=0.0, value=0.0, step=0.1)
            new_low = st.number_input("Low stock threshold", min_value=0.0, value=10.0, step=1.0)
            has_exp = st.checkbox("Has expiration date?", value=False)
            new_exp = st.date_input("Expiration date", value=date.today()) if has_exp else None

        if st.form_submit_button("➕ Add reagent", type="primary", use_container_width=True):
            if NA(new_name) == "N/A":
                st.error("Name is required (cannot be blank).")
                st.stop()

            payload = {
                "id": NA(new_id),
                "name": NA(new_name),
                "cas_number": NA(new_cas),
                "supplier": NA(new_supplier),
                "location": NA(new_location),
                "quantity": str(new_qty),
                "unit": NA(new_unit),
                "expiration_date": (new_exp.isoformat() if new_exp else "N/A"),
                "low_stock_threshold": str(new_low),
            }
            append_row(payload)
            audit("CREATE", 0, payload["name"], json.dumps(payload, ensure_ascii=False))
            st.success("Reagent added.")
            load_inventory.clear()
            st.rerun()

    st.divider()
    st.markdown("### Import from CSV (append into Google Sheet)")

    csv_file = st.file_uploader(
        "Upload CSV file",
        type=["csv"],
        accept_multiple_files=False,
        key="import_csv"
    )

    if csv_file is not None:
        try:
            df_csv = pd.read_csv(csv_file)
            st.success(f"Loaded CSV: {df_csv.shape[0]} rows × {df_csv.shape[1]} columns")
            st_dataframe_safe(df_csv.head(50))
        except Exception as e:
            st.error(f"Failed to read CSV: {e}")
            df_csv = None

        if df_csv is not None:
            if st.checkbox("I confirm I want to append these rows into Google Sheets", key="confirm_csv_import"):
                if st.button("🚀 Import CSV → Append to Inventory", type="primary", use_container_width=True):
                    try:
                        if not RAW_HEADERS:
                            st.error("Cannot import: inventory sheet header row is missing.")
                            st.stop()

                        rows_to_append = csv_to_inventory_rows(df_csv, RAW_HEADERS)

                        append_rows_bulk(rows_to_append)

                        audit("CSV_IMPORT", 0, "BULK_IMPORT", f"Imported {len(rows_to_append)} rows from CSV")
                        st.success(f"Imported {len(rows_to_append)} rows into '{WORKSHEET_INVENTORY}'!")
                        load_inventory.clear()
                        st.rerun()

                    except Exception as e:
                        st.error(f"CSV import failed: {e}")

def render_inventory_check(df: pd.DataFrame):
    render_topbar("Inventory Check")

    st.markdown("### 📉 Log Usage (deduct from stock)")
    if df.empty:
        st.info("No inventory loaded.")
        return

    q = st.text_input("Search by Name or CAS Number", "")

    df2 = df.copy()
    if q.strip():
        q2 = q.strip()
        mask = (
            df2["name"].astype(str).str.contains(q2, case=False, na=False)
            | df2["cas_number"].astype(str).str.contains(q2, case=False, na=False)
        )
        df2 = df2[mask].reset_index(drop=True)

    if df2.empty:
        st.warning("No matching reagents.")
        return

    max_show = min(20, len(df2))
    df_show = df2.head(max_show).copy()

    options = []
    for i, r in df_show.iterrows():
        name = str(r.get("name", ""))
        cas = str(r.get("cas_number", ""))
        loc = str(r.get("location", ""))
        qty = float(r.get("quantity", 0) or 0)
        unit = str(r.get("unit", ""))
        rownum = int(r.get("_row", 0))
        options.append((i, f"{name} | CAS: {cas} | {loc} | Qty: {qty:.2f} {unit} | row {rownum}"))

    sel_i = st.selectbox("Select reagent", options, format_func=lambda x: x[1])
    idx = sel_i[0]
    r = df_show.loc[idx]

    rownum = int(r.get("_row", 0))
    name = str(r.get("name", "N/A"))
    cas = str(r.get("cas_number", "N/A"))
    loc = str(r.get("location", "N/A"))
    unit = str(r.get("unit", "N/A"))

    qty_before = float(r.get("quantity", 0) or 0)

    st.markdown("#### Enter usage")
    with st.form("usage_form"):
        amount_used = st.number_input(
            f"Amount used ({unit})",
            min_value=0.0,
            value=0.0,
            step=0.1
        )
        notes = st.text_input("Notes (optional)", "")

        allow_negative = st.checkbox("Allow negative stock (not recommended)", value=False)

        submitted = st.form_submit_button("✅ Submit & Deduct", type="primary")
        if submitted:
            if amount_used <= 0:
                st.error("Amount used must be > 0.")
                st.stop()

            if rownum <= 1:
                st.error("Internal error: missing sheet row number.")
                st.stop()

            if not try_lock_row(rownum, st.session_state.username, "usage_deduct"):
                st.error("This reagent is being edited by someone else. Try again in ~1 minute.")
                st.stop()

            qty_after = qty_before - float(amount_used)

            if (qty_after < 0) and (not allow_negative):
                st.error(f"Not enough stock. Current: {qty_before:.2f} {unit}, attempted use: {amount_used:.2f} {unit}.")
                st.stop()

            update_row_cells(rownum, {"quantity": str(qty_after)})

            ts = now_utc().isoformat()
            details = {
                "reagent": name,
                "cas_number": cas,
                "row": rownum,
                "amount_used": amount_used,
                "unit": unit,
                "qty_before": qty_before,
                "qty_after": qty_after,
                "location": loc,
                "notes": notes
            }
            audit("USAGE_DEDUCT", rownum, name, json.dumps(details, ensure_ascii=False))

            append_usage_log(
                ts_iso=ts,
                user=st.session_state.username,
                role=st.session_state.role,
                action="USAGE_DEDUCT",
                name=name,
                cas=cas,
                amount_used=amount_used,
                unit=unit,
                qty_before=qty_before,
                qty_after=qty_after,
                location=loc,
                notes=notes
            )

            st.success(f"Deducted {amount_used:.2f} {unit} from **{name}**. New qty: {qty_after:.2f} {unit}")
            load_inventory.clear()
            st.rerun()

def render_barcode_scan():
    render_topbar("Barcode Scan")

    st.markdown("### 📷 Scan & Add (Mobile-first)")
    st.write("Step 1: Take a photo (recommended) or upload. Step 2: OCR (cloud) → auto-fill. Step 3: Add.")

    if not vision_available():
        st.warning(
            "Cloud OCR is not configured. Add to Streamlit Secrets:\n\n"
            "[gcp_vision]\napi_key = \"YOUR_GOOGLE_VISION_API_KEY\"\n\n"
            "You can still capture/upload and manually fill fields below.",
            icon="⚠️"
        )

    st.markdown("#### Step 1 — Capture / Upload")

    cam = st.camera_input("📸 Take a photo of the label (best on phone)", key="scan_cam")
    if cam is not None:
        b = cam.getvalue()
        set_scan_image_bytes(
            b,
            source="camera",
            meta={"filename": "camera.jpg", "mime_type": getattr(cam, "type", "image/jpeg"), "size_bytes": len(b)}
        )
        st.success("Camera image captured ✅ (stored for this session)")

    up = st.file_uploader(
        "⬆️ Or upload a photo (JPG/PNG recommended)",
        type=["jpg", "jpeg", "png", "heic", "heif"],
        accept_multiple_files=False,
        key="scan_uploader",
    )
    if up is not None:
        b = up.getvalue()
        set_scan_image_bytes(
            b,
            source="upload",
            meta={"filename": up.name, "mime_type": up.type, "size_bytes": up.size}
        )
        st.success("File received ✅ (stored for this session)")

    image_bytes = st.session_state.get("scan_image_bytes")

    if not image_bytes:
        st.info("No image stored yet. Use Camera or Upload above.")
        return

    st.markdown("#### Preview")
    if PIL_AVAILABLE:
        try:
            img = Image.open(io.BytesIO(image_bytes))
            st_image_safe(img, caption=f"Source: {st.session_state.get('scan_image_source','?')}")
        except Exception as e:
            st.error(f"Cannot open image for preview (preview only). Error: {e}")
    else:
        st.info("Pillow not available → skipping preview (OCR can still run).")

    st.markdown("#### Step 2 — OCR (Cloud)")

    cols = st.columns([1, 1])
    with cols[0]:
        auto_crop = st.checkbox("🧪 Auto-crop label before OCR (improves accuracy)", value=True, disabled=(not PIL_AVAILABLE or not vision_available()))
    with cols[1]:
        show_ocr = st.checkbox("Show OCR text (debug)", value=False)

    if st.button("🔎 Run Cloud OCR", type="primary", use_container_width=True, disabled=not vision_available()):
        try:
            with st.spinner("Running Vision OCR..."):
                bytes_for_ocr = autocrop_bytes_using_vision(image_bytes) if auto_crop else image_bytes
                resp = vision_text_detection(bytes_for_ocr)
                text = vision_extract_full_text(resp)

            st.session_state["ocr_text"] = text
            st.session_state["scan_fields"] = parse_reagent_fields(text)
            st.success('OCR complete. Review fields below (missing values are "N/A").')

            if auto_crop and PIL_AVAILABLE and bytes_for_ocr != image_bytes:
                try:
                    img2 = Image.open(io.BytesIO(bytes_for_ocr))
                    st_image_safe(img2, caption="Auto-cropped image used for OCR")
                except Exception:
                    pass

            if show_ocr:
                st.text_area("OCR output", text, height=200)

        except Exception as e:
            st.error(f"OCR failed: {e}")

    st.markdown("#### Step 3 — Review & Add")

    fields = st.session_state.get("scan_fields") or {
        "id": "N/A", "name": "N/A", "cas_number": "N/A", "supplier": "N/A",
        "location": "N/A", "quantity": "0", "unit": "N/A", "expiration_date": "N/A"
    }

    with st.form("scan_add_form"):
        c1, c2, c3 = st.columns(3)

        with c1:
            scan_id = st.text_input("ID", value=fields.get("id", "N/A"))
            scan_name = st.text_input("Name", value=fields.get("name", "N/A"))
            scan_cas = st.text_input("CAS Number", value=fields.get("cas_number", "N/A"))

        with c2:
            scan_supplier = st.text_input("Supplier", value=fields.get("supplier", "N/A"))
            scan_location = location_widget("Location", value=fields.get("location", "N/A"), key="scan_loc")
            scan_unit = st.text_input("Unit", value=fields.get("unit", "N/A"))

        with c3:
            try:
                qty_default = float(fields.get("quantity", "0") or 0)
            except Exception:
                qty_default = 0.0
            scan_qty = st.number_input("Quantity", min_value=0.0, value=qty_default, step=0.1)

            scan_low = st.number_input("Low stock threshold", min_value=0.0, value=10.0, step=1.0)

            exp_str = fields.get("expiration_date", "N/A")
            has_exp = st.checkbox("Has expiration date?", value=(exp_str != "N/A"))
            scan_exp = None
            if has_exp:
                dt = pd.to_datetime(exp_str, errors="coerce")
                scan_exp = st.date_input("Expiration date", value=(dt.date() if pd.notnull(dt) else date.today()))

        if st.form_submit_button("➕ Add reagent", type="primary", use_container_width=True):
            if NA(scan_name) == "N/A":
                st.error("Name is required. Please type it if OCR missed it.")
                st.stop()

            payload = {
                "id": NA(scan_id),
                "name": NA(scan_name),
                "cas_number": NA(scan_cas),
                "supplier": NA(scan_supplier),
                "location": NA(scan_location),
                "quantity": str(scan_qty),
                "unit": NA(scan_unit),
                "expiration_date": (scan_exp.isoformat() if scan_exp else "N/A"),
                "low_stock_threshold": str(scan_low),
            }

            append_row(payload)
            audit("CREATE_SCAN", 0, payload["name"], json.dumps(payload, ensure_ascii=False))

            st.success("Reagent added from scan.")
            load_inventory.clear()
            st.session_state.pop("scan_fields", None)
            st.session_state.pop("ocr_text", None)
            clear_scan_image_bytes()
            st.rerun()

    c = st.columns([1, 2])
    with c[0]:
        if st.button("🧹 Clear image + OCR", use_container_width=True):
            clear_scan_image_bytes()
            st.session_state.pop("scan_fields", None)
            st.session_state.pop("ocr_text", None)
            st.rerun()
    with c[1]:
        st.caption("Tip: For best OCR, fill the screen with the label, good lighting, no glare.")

def render_admin():
    render_topbar("Admin")

    st.markdown("### Admin")
    st.subheader("Runtime info")
    st.write("Streamlit version:", st.__version__)
    st.write("Pillow available:", PIL_AVAILABLE)
    st.write("Cloud OCR configured:", vision_available())
    st.write("Locks sheet enabled:", LOCKS_ENABLED)
    st.write("Audit sheet enabled:", AUDIT_ENABLED)

    st.subheader("Secrets required for Cloud OCR")
    st.code('[gcp_vision]\napi_key = "YOUR_GOOGLE_VISION_API_KEY"\n', language="toml")

    st.subheader("Recommended sheet headers")
    st.code(
        "Inventory (template) header row:\n"
        + " | ".join(EXPECTED_COLS)
        + "\n\nLocks (locks) header row:\n"
          "row | locked_by | locked_until_iso | purpose\n\n"
          "Audit (audit_log) header row:\n"
          "ts_iso | user | role | action | row | reagent_name | details\n",
        language="text"
    )

# Drive the app
page = sidebar_nav()

# Keep the old warning banner but show it on dashboard only
if page == "Dashboard":
    # show existing warning banner (low_alerts/expired_alerts computed earlier)
    pass

if page == "Dashboard":
    render_dashboard(reagents_df)
elif page == "All Reagents":
    render_all_reagents(reagents_df)
elif page == "Inbound":
    render_inbound_add()
elif page == "Barcode Scan":
    render_barcode_scan()
elif page == "Inventory Check":
    render_inventory_check(reagents_df)
elif page == "Admin":
    render_admin()

st.caption("LabTrack • Streamlit + Google Sheets • January 2026")
