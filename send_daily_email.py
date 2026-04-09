"""
Daily Email Sender — SAP Job Listings
Uses Microsoft Graph API (Azure app permissions — no SMTP needed).
Triggered by GitHub Actions (see .github/workflows/*.yml) — run directly with no arguments.

Required secrets / .env:
    SUPABASE_URL, SUPABASE_KEY
    AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET
    EMAIL_FROM   — the mailbox to send from  (e.g. automation@yourcompany.com)
    EMAIL_TO     — comma-separated recipients
    EMAIL_CC     — comma-separated CC (optional)
"""

import os
import json
import io
import base64
import logging
import time
from datetime import datetime, date, timedelta

import requests
from dotenv import load_dotenv
from supabase import create_client

# ─────────────────────────────────────────────
# ENV
# ─────────────────────────────────────────────
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

SUPABASE_URL        = os.getenv("SUPABASE_URL")
SUPABASE_KEY        = os.getenv("SUPABASE_KEY")
AZURE_TENANT_ID     = os.getenv("AZURE_TENANT_ID")
AZURE_CLIENT_ID     = os.getenv("AZURE_CLIENT_ID")
AZURE_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
EMAIL_FROM          = os.getenv("EMAIL_FROM")
EMAIL_TO            = os.getenv("EMAIL_TO", "")
EMAIL_CC            = os.getenv("EMAIL_CC", "")

for _var, _val in {
    "SUPABASE_URL": SUPABASE_URL, "SUPABASE_KEY": SUPABASE_KEY,
    "AZURE_TENANT_ID": AZURE_TENANT_ID, "AZURE_CLIENT_ID": AZURE_CLIENT_ID,
    "AZURE_CLIENT_SECRET": AZURE_CLIENT_SECRET,
    "EMAIL_FROM": EMAIL_FROM, "EMAIL_TO": EMAIL_TO,
}.items():
    if not _val:
        raise EnvironmentError(f"Missing required env var: {_var}")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# ─────────────────────────────────────────────
# GRAPH AUTH
# ─────────────────────────────────────────────
_token_cache = {"token": None, "expires_at": 0}


def get_graph_token() -> str:
    """Obtain (or return cached) Microsoft Graph access token via client credentials."""
    now = time.time()
    if _token_cache["token"] and now < _token_cache["expires_at"] - 60:
        return _token_cache["token"]

    resp = requests.post(
        f"https://login.microsoftonline.com/{AZURE_TENANT_ID}/oauth2/v2.0/token",
        data={
            "grant_type":    "client_credentials",
            "client_id":     AZURE_CLIENT_ID,
            "client_secret": AZURE_CLIENT_SECRET,
            "scope":         "https://graph.microsoft.com/.default",
        },
        timeout=20,
    )
    resp.raise_for_status()
    data = resp.json()
    _token_cache["token"]      = data["access_token"]
    _token_cache["expires_at"] = now + data.get("expires_in", 3600)
    logging.info("Graph access token acquired.")
    return _token_cache["token"]


# ─────────────────────────────────────────────
# DATA HELPERS
# ─────────────────────────────────────────────
COLUMNS_FROM_DB = [
    "jr_no",
    "skill_name",
    "posting_start_date",
    "posting_end_date",
    "client_recruiter",
    "recruiter_email",
    "job_details",
    "company_name",
    "jr_status",
]


DISPLAY_HEADERS = {
    "jr_no":               "JR No",
    "skill_name":          "Job Title",
    "posting_start_date":  "Posting Start Date",
    "posting_end_date":    "Posting End Date",
    "client_recruiter":    "Client Recruiter",
    "recruiter_email":     "Recruiter Email",
    "job_details":         "Job Details",
    "company_name":        "Company",
    "jr_status":           "JR Status",
}


def fetch_active_jobs() -> list:
    """Pull active + new jr records (posting_end_date >= today, or still flagged new jr)."""
    today_iso = date.today().isoformat()
    resp = (
        supabase.table("jr_master")
        .select(", ".join(COLUMNS_FROM_DB))
        .gte("posting_end_date", today_iso)
        .order("posting_start_date", desc=True)
        .limit(5000)
        .execute()
    )
    return resp.data or []


def count_new_jrs(records: list) -> int:
    """Count records currently flagged 'new jr' in DB (not yet emailed)."""
    return sum(1 for r in records if str(r.get("jr_status", "")).strip().lower() == "new jr")


def count_yesterday_new_jrs() -> int:
    """Count records that were marked 'new jr' and emailed yesterday.
    Since we reset 'new jr' -> 'active' after each send, we use modified_date
    on the day *before* today to find yesterday's batch.
    """
    yesterday_start = (date.today() - timedelta(days=1)).isoformat() + "T00:00:00"
    yesterday_end   = (date.today() - timedelta(days=1)).isoformat() + "T23:59:59"
    try:
        resp = (
            supabase.table("jr_master")
            .select("jr_no")
            .gte("modified_date", yesterday_start)
            .lte("modified_date", yesterday_end)
            .eq("jr_status", "active")   # already reset to active after yesterday's email
            .execute()
        )
        return len(resp.data or [])
    except Exception as e:
        logging.warning(f"Could not fetch yesterday count: {e}")
        return 0


def reset_new_jr_to_active() -> None:
    """After a successful email send, flip all 'new jr' records back to 'active'.
    This clears the flag so the next run only highlights genuinely new records.
    """
    now_iso = datetime.now().isoformat()
    try:
        resp = supabase.table("jr_master").select("jr_no").eq("jr_status", "new jr").execute()
        jr_nos = [r["jr_no"] for r in (resp.data or [])]
        if not jr_nos:
            logging.info("reset_new_jr_to_active: nothing to reset")
            return

        batch_size = 50
        for i in range(0, len(jr_nos), batch_size):
            batch = jr_nos[i: i + batch_size]
            supabase.table("jr_master").update(
                {"jr_status": "active", "modified_date": now_iso}
            ).in_("jr_no", batch).execute()
            logging.info(f"  Reset to active batch {i // batch_size + 1}: {len(batch)} records")

        logging.info(f"reset_new_jr_to_active: {len(jr_nos)} records reset to 'active'")
    except Exception as e:
        logging.error(f"reset_new_jr_to_active failed: {e}")


HANDOFF_FILE = "scraper_handoff.json"


def _load_handoff() -> dict:
    """Read the JSON file written by the scraper. Returns empty lists if missing."""
    try:
        with open(HANDOFF_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        logging.warning(f"Handoff file '{HANDOFF_FILE}' not found — highlights will be empty")
        return {"new_jr_nos": [], "deactivated_jr_nos": []}
    except Exception as e:
        logging.warning(f"Could not read handoff file: {e}")
        return {"new_jr_nos": [], "deactivated_jr_nos": []}


def _fetch_rows_by_jr_nos(jr_nos: list) -> list:
    """Fetch jr_no, skill_name, client_recruiter, jr_status for a list of jr_nos."""
    if not jr_nos:
        return []
    cols = "jr_no, skill_name, client_recruiter, jr_status"
    try:
        resp = (
            supabase.table("jr_master")
            .select(cols)
            .in_("jr_no", jr_nos)
            .order("jr_no")
            .execute()
        )
        return resp.data or []
    except Exception as e:
        logging.warning(f"_fetch_rows_by_jr_nos failed: {e}")
        return []


def fetch_highlights() -> dict:
    """Use the scraper handoff file to fetch exactly the new/deactivated rows."""
    handoff    = _load_handoff()
    new_rows   = _fetch_rows_by_jr_nos(handoff["new_jr_nos"])
    deact_rows = _fetch_rows_by_jr_nos(handoff["deactivated_jr_nos"])
    return {"new_jr": new_rows, "deactivated": deact_rows}


def fetch_summary_counts(highlights: dict) -> dict:
    """Derive deactivated count from handoff; query DB for active + new jr counts."""
    try:
        active_resp = supabase.table("jr_master").select("jr_no").eq("jr_status", "active").execute()
        active_count = len(active_resp.data or [])
    except Exception as e:
        logging.warning(f"fetch_summary active count failed: {e}")
        active_count = 0

    try:
        new_resp = supabase.table("jr_master").select("jr_no").eq("jr_status", "new jr").execute()
        new_count = len(new_resp.data or [])
    except Exception as e:
        logging.warning(f"fetch_summary new jr count failed: {e}")
        new_count = 0

    # Deactivated count comes from the handoff — exact, no modified_date ambiguity
    deact_count = len(highlights.get("deactivated", []))

    return {"active": active_count, "new_jr": new_count, "deactivated": deact_count}


# ─────────────────────────────────────────────
# EXCEL BUILDER
# ─────────────────────────────────────────────
STATUS_COLORS = {
    "new jr":   "00B050",   # green
    "active":   "0070C0",   # blue
    "inactive": "FF0000",   # red
}


def build_excel(records: list) -> bytes:
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    wb = Workbook()
    ws = wb.active
    ws.title = "Job Listings"

    thin   = Side(style="thin", color="D0D0D0")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    # ── Header ──
    header_fill  = PatternFill("solid", fgColor="1F4E79")
    header_font  = Font(bold=True, color="FFFFFF", size=11)
    header_align = Alignment(horizontal="center", vertical="center", wrap_text=True)

    for col_idx, col_key in enumerate(COLUMNS_FROM_DB, start=1):
        cell = ws.cell(row=1, column=col_idx, value=DISPLAY_HEADERS.get(col_key, col_key))
        cell.font      = header_font
        cell.fill      = header_fill
        cell.alignment = header_align
        cell.border    = border
    ws.row_dimensions[1].height = 28

    # ── Data ──
    alt_a = PatternFill("solid", fgColor="EBF3FB")
    alt_b = PatternFill("solid", fgColor="FFFFFF")

    for row_idx, record in enumerate(records, start=2):
        row_fill   = alt_a if row_idx % 2 == 0 else alt_b
        status_raw = str(record.get("jr_status", "")).strip().lower()

        for col_idx, col_key in enumerate(COLUMNS_FROM_DB, start=1):
            value = record.get(col_key, "")
            if value is None or str(value) == "None":
                value = ""

            cell = ws.cell(row=row_idx, column=col_idx, value=value)
            cell.alignment = Alignment(vertical="center", wrap_text=(col_key == "job_details"))
            cell.border    = border

            if col_key == "jr_status":
                # DB holds 'new jr' until email is sent, then resets to 'active'
                cell.fill      = PatternFill("solid", fgColor=STATUS_COLORS.get(status_raw, "808080"))
                cell.font      = Font(bold=True, color="FFFFFF", size=10)
                cell.alignment = Alignment(horizontal="center", vertical="center")
            else:
                cell.fill = row_fill

    col_widths = {
        "jr_no": 12, "skill_name": 30, "posting_start_date": 18,
        "posting_end_date": 18, "client_recruiter": 22,
        "recruiter_email": 28, "job_details": 50,
        "company_name": 14, "jr_status": 14,
    }
    for col_idx, col_key in enumerate(COLUMNS_FROM_DB, start=1):
        ws.column_dimensions[get_column_letter(col_idx)].width = col_widths.get(col_key, 16)

    ws.freeze_panes    = "A2"
    ws.auto_filter.ref = ws.dimensions

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ─────────────────────────────────────────────
# HTML BODY
# ─────────────────────────────────────────────
def _highlights_table_html(rows: list, status: str) -> str:
    """Build an HTML table for new jr or deactivated rows."""
    if not rows:
        return ""
    color   = "#00B050" if status == "new jr" else "#CC0000"
    label   = "New JR" if status == "new jr" else "Deactivated Today"
    bg      = "#f0fff4" if status == "new jr" else "#fff5f5"
    rows_html = ""
    for r in rows:
        recruiter = r.get("client_recruiter") or "—"
        rows_html += (
            f"<tr>"
            f"<td style='padding:6px 10px;border-bottom:1px solid #eee;font-size:12.5px;'>{r.get('jr_no','')}</td>"
            f"<td style='padding:6px 10px;border-bottom:1px solid #eee;font-size:12.5px;'>{r.get('skill_name','')}</td>"
            f"<td style='padding:6px 10px;border-bottom:1px solid #eee;font-size:12.5px;'>{recruiter}</td>"
            f"<td style='padding:6px 10px;border-bottom:1px solid #eee;font-size:12.5px;text-align:center;'>"
            f"<span style='background:{color};color:#fff;border-radius:10px;padding:2px 10px;font-size:11.5px;font-weight:700;'>{label}</span>"
            f"</td>"
            f"</tr>"
        )
    return f"""
<p style="margin:18px 0 6px;font-size:13.5px;font-weight:600;color:#333;">{label} — {len(rows)} record(s)</p>
<table style="width:100%;border-collapse:collapse;background:{bg};border-radius:6px;overflow:hidden;font-family:'Segoe UI',Arial,sans-serif;">
  <thead>
    <tr style="background:{color};">
      <th style="padding:8px 10px;text-align:left;color:#fff;font-size:12px;font-weight:600;">JR No</th>
      <th style="padding:8px 10px;text-align:left;color:#fff;font-size:12px;font-weight:600;">Job Title</th>
      <th style="padding:8px 10px;text-align:left;color:#fff;font-size:12px;font-weight:600;">Recruiter</th>
      <th style="padding:8px 10px;text-align:center;color:#fff;font-size:12px;font-weight:600;">Status</th>
    </tr>
  </thead>
  <tbody>{rows_html}</tbody>
</table>"""


def build_html_body(summary: dict, highlights: dict) -> str:
    today_str   = datetime.now().strftime("%B %d, %Y")
    active_cnt  = summary.get("active", 0)
    new_cnt     = summary.get("new_jr", 0)
    deact_cnt   = summary.get("deactivated", 0)

    new_table   = _highlights_table_html(highlights.get("new_jr", []),    "new jr")
    deact_table = _highlights_table_html(highlights.get("deactivated", []), "inactive")

    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <style>
    body {{ font-family:'Segoe UI',Arial,sans-serif; background:#f4f6f9; margin:0; padding:0; }}
    .wrapper {{ max-width:640px; margin:30px auto; background:#fff; border-radius:8px;
                overflow:hidden; box-shadow:0 2px 8px rgba(0,0,0,.12); }}
    .header  {{ background:#1F4E79; padding:22px 30px; color:#fff; }}
    .header h2 {{ margin:0; font-size:18px; letter-spacing:.5px; }}
    .header p  {{ margin:4px 0 0; font-size:13px; color:#cde0f5; }}
    .body    {{ padding:28px 30px; color:#333; font-size:14.5px; line-height:1.7; }}
    .summary-cards {{ display:flex; gap:12px; margin:18px 0; }}
    .card {{ flex:1; border-radius:8px; padding:14px 16px; text-align:center; }}
    .card-label {{ font-size:11.5px; font-weight:600; text-transform:uppercase;
                   letter-spacing:.5px; margin-bottom:6px; }}
    .card-value {{ font-size:26px; font-weight:700; }}
    .card-active   {{ background:#e8f0fb; }}
    .card-active   .card-label {{ color:#1a56a0; }}
    .card-active   .card-value {{ color:#1a56a0; }}
    .card-new      {{ background:#e8f5e9; }}
    .card-new      .card-label {{ color:#1e7e34; }}
    .card-new      .card-value {{ color:#1e7e34; }}
    .card-deact    {{ background:#fdecea; }}
    .card-deact    .card-label {{ color:#b71c1c; }}
    .card-deact    .card-value {{ color:#b71c1c; }}
    .legend {{ display:flex; gap:16px; margin:18px 0; flex-wrap:wrap; }}
    .legend-item {{ display:flex; align-items:center; gap:6px; font-size:13px; color:#444; }}
    .dot {{ width:12px; height:12px; border-radius:50%; display:inline-block; }}
    .dot-new      {{ background:#00B050; }}
    .dot-active   {{ background:#0070C0; }}
    .dot-inactive {{ background:#FF0000; }}
    .footer {{ background:#f0f4f8; padding:16px 30px; border-top:1px solid #dde4ee;
               font-size:12.5px; color:#666; }}
    .footer strong {{ color:#1F4E79; font-size:13.5px; }}
  </style>
</head>
<body>
<div class="wrapper">
  <div class="header">
    <h2>&#128203; Daily Job Listings Report</h2>
    <p>{today_str}</p>
  </div>
  <div class="body">
    <p>Dear Team,</p>
    <p>Please find attached the <strong>data extract</strong> as requested, along with
       today's <strong>active job listings</strong> (posting end date &gt; today).</p>

    <div class="summary-cards">
      <div class="card card-active">
        <div class="card-label">Active JRs</div>
        <div class="card-value">{active_cnt}</div>
      </div>
      <div class="card card-new">
        <div class="card-label">New JRs</div>
        <div class="card-value">{new_cnt}</div>
      </div>
      <div class="card card-deact">
        <div class="card-label">Deactivated Today</div>
        <div class="card-value">{deact_cnt}</div>
      </div>
    </div>

    {new_table}
    {deact_table}

    <p style="margin-top:20px;"><strong>Excel Status Legend:</strong></p>
    <div class="legend">
      <span class="legend-item"><span class="dot dot-new"></span> New JR &ndash; Newly added posting</span>
      <span class="legend-item"><span class="dot dot-active"></span> Active &ndash; Posting end date in future</span>
      <span class="legend-item"><span class="dot dot-inactive"></span> Inactive &ndash; Expired / not in latest extract</span>
    </div>
    <p>For full job details and requirements, please refer to the
       <a href="https://hr-data-ui-volibits.streamlit.app/#job-requirements" style="color:#1F4E79;font-weight:600;">ATS Portal</a>.</p>
    <p>If any clarification is required, our automation desk remains at your service.</p>
  </div>
  <div class="footer">
    <strong>TalentAxis</strong><br>
    For Talent Management Automations
  </div>
</div>
</body>
</html>"""


# ─────────────────────────────────────────────
# SEND VIA MICROSOFT GRAPH
# ─────────────────────────────────────────────
def send_email():
    logging.info("=== Starting daily email job ===")

    # 1. Fetch data
    records    = fetch_active_jobs()
    highlights = fetch_highlights()
    summary    = fetch_summary_counts(highlights)
    logging.info(
        f"Records: {len(records)} | Active: {summary['active']} | "
        f"New JR: {summary['new_jr']} | Deactivated today: {summary['deactivated']}"
    )

    # 2. Excel attachment
    excel_bytes = build_excel(records)
    excel_name  = f"job_listings_{datetime.now().strftime('%Y%m%d')}.xlsx"
    excel_b64   = base64.b64encode(excel_bytes).decode("utf-8")
    logging.info(f"Excel built: {len(excel_bytes):,} bytes → {excel_name}")

    # 3. Subject  e.g. "JR Data for BS - 08 Apr 2026"
    subject = f"JR Data for BS - {datetime.now().strftime('%d %b %Y')}"

    # 4. Graph payload
    to_recipients = [
        {"emailAddress": {"address": a.strip()}}
        for a in EMAIL_TO.split(",") if a.strip()
    ]
    cc_recipients = [
        {"emailAddress": {"address": a.strip()}}
        for a in EMAIL_CC.split(",") if a.strip()
    ]

    payload = {
        "message": {
            "subject": subject,
            "body": {"contentType": "HTML", "content": build_html_body(summary, highlights)},
            "toRecipients": to_recipients,
            **({"ccRecipients": cc_recipients} if cc_recipients else {}),
            "attachments": [{
                "@odata.type":  "#microsoft.graph.fileAttachment",
                "name":         excel_name,
                "contentType":  "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "contentBytes": excel_b64,
            }],
        },
        "saveToSentItems": "true",
    }

    # 5. Send
    token   = get_graph_token()
    url     = f"https://graph.microsoft.com/v1.0/users/{EMAIL_FROM}/sendMail"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    for attempt in range(3):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=60)
            if resp.status_code == 202:
                logging.info(f"✅ Email sent | Subject: '{subject}'")
                logging.info(f"   To: {EMAIL_TO}" + (f" | CC: {EMAIL_CC}" if EMAIL_CC else ""))
                # Reset 'new jr' → 'active' ONLY after confirmed send
                reset_new_jr_to_active()
                break
            else:
                logging.error(f"Attempt {attempt+1}: {resp.status_code} — {resp.text}")
                if attempt < 2:
                    time.sleep(5)
        except Exception as e:
            logging.error(f"Attempt {attempt+1}: {e}")
            if attempt < 2:
                time.sleep(5)
    else:
        raise RuntimeError("All 3 Graph API send attempts failed.")

    logging.info("=== Daily email job complete ===")


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    send_email()