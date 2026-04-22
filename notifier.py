"""
notifier.py
-----------
Sends email alerts via Gmail SMTP for:
  1. ITT / OC batch updates  (send_itt_oc_alert)
  2. SPOM slot availability   (send_spom_alert)

Prerequisites (env vars / Railway variables):
  GMAIL_USER     → your Gmail address  (e.g. yourname@gmail.com)
  GMAIL_APP_PASS → 16-char App Password (NOT your Gmail login password)

To create a Gmail App Password:
  1. Enable 2-Factor Authentication on your Google account
  2. Go to: Google Account → Security → App Passwords
  3. Generate one for "Mail" → label doesn't matter
  4. Copy the 16-char code into your environment variable

Per-user emails
───────────────
Both send_itt_oc_alert() and send_spom_alert() accept a `to_email`
parameter so each subscribed user gets their own alert.  The ALERT_EMAIL
env var is still used as the fallback when no per-user email is supplied
(e.g. from monitor.py).
"""

import html
import logging
import os
import smtplib
from datetime import datetime, timezone, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)

# IST = UTC+5:30
IST = timezone(timedelta(hours=5, minutes=30))

_BRAND_COLOR = "#1a3c6e"
_ACCENT      = "#e8f0fb"


# ─── Shared helpers ───────────────────────────────────────────────────────────

def _esc(s) -> str:
    return html.escape(str(s))


def _now_ist() -> str:
    return datetime.now(IST).strftime("%d %b %Y, %I:%M %p IST")


def _wrap_html(title: str, subtitle: str, body_html: str, cta_url: str, cta_label: str) -> str:
    """
    Standard branded HTML email template.
    Inserts title, subtitle, a body section and a CTA button.
    """
    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="font-family:Arial,sans-serif;margin:0;padding:0;background:#f5f5f5">
  <div style="max-width:700px;margin:30px auto;background:#fff;border-radius:8px;
              overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.12)">

    <div style="background:{_BRAND_COLOR};padding:24px 32px">
      <h1 style="color:#fff;margin:0;font-size:22px">{title}</h1>
      <p  style="color:#a8c4e8;margin:6px 0 0;font-size:14px">{subtitle}</p>
    </div>

    {body_html}

    <div style="padding:0 32px 32px;text-align:center">
      <a href="{cta_url}"
         style="display:inline-block;background:{_BRAND_COLOR};color:#fff;
                padding:12px 28px;border-radius:6px;text-decoration:none;
                font-weight:bold;font-size:15px">{cta_label}</a>
    </div>

    <div style="background:#f5f5f5;padding:14px 32px;text-align:center">
      <p style="color:#aaa;font-size:12px;margin:0">
        ICAI Monitor · Automated alert · Reply STOP to this email to unsubscribe.
      </p>
    </div>

  </div>
</body>
</html>"""


def _send_email(to_email: str, subject: str, plain: str, html_body: str):
    """
    Core SMTP send via Gmail SSL.
    Reads GMAIL_USER and GMAIL_APP_PASS from environment.
    Raises EnvironmentError if credentials are missing.
    Raises smtplib / socket exceptions on delivery failure.
    """
    gmail_user = os.environ.get("GMAIL_USER")
    gmail_pass = os.environ.get("GMAIL_APP_PASS")

    if not gmail_user or not gmail_pass:
        raise EnvironmentError(
            "Missing GMAIL_USER or GMAIL_APP_PASS environment variables. "
            "Set them in Railway dashboard / .env file."
        )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"ICAI Monitor <{gmail_user}>"
    msg["To"]      = to_email
    msg.attach(MIMEText(plain,     "plain"))
    msg.attach(MIMEText(html_body, "html"))

    logger.info(f"[Email] Sending '{subject}' → {to_email}")
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as srv:
        srv.login(gmail_user, gmail_pass)
        srv.sendmail(gmail_user, to_email, msg.as_string())
    logger.info(f"[Email] Delivered → {to_email}")


# ─── ITT / OC batch alert ─────────────────────────────────────────────────────

def _build_itt_oc_html(batches: list[dict], region: str, pou: str, course: str) -> tuple[str, str]:
    """Build plain-text + HTML for an ITT/OC batch change alert."""
    timestamp = _now_ist()

    # Plain text
    lines = [
        "ICAI BATCH ALERT",
        "=" * 50,
        "New/updated batch(es) detected for your preferences:",
        f"  Region : {region}",
        f"  PoU    : {pou}",
        f"  Course : {course}",
        f"  Checked: {timestamp}",
        "",
        "BATCH DETAILS:",
        "-" * 50,
    ]
    for i, b in enumerate(batches, 1):
        lines.append(f"\nBatch #{i}")
        for k, v in b.items():
            lines.append(f"  {k}: {v}")
    lines += [
        "",
        "-" * 50,
        "Register here: https://www.icaionlineregistration.org/launchbatchdetail.aspx",
        "",
        "— ICAI Batch Monitor (automated alert)",
    ]
    plain = "\n".join(lines)

    # Build table rows
    table_rows = ""
    if batches:
        headers      = list(batches[0].keys())
        header_cells = "".join(
            f"<th style='padding:8px 12px;background:{_BRAND_COLOR};"
            f"color:#fff;text-align:left'>{_esc(h)}</th>"
            for h in headers
        )
        table_rows = f"<tr>{header_cells}</tr>"
        for i, b in enumerate(batches):
            bg    = _ACCENT if i % 2 == 0 else "#fff"
            cells = "".join(
                f"<td style='padding:8px 12px;border-bottom:1px solid #e0e0e0'>"
                f"{_esc(b.get(h, ''))}</td>"
                for h in headers
            )
            table_rows += f"<tr style='background:{bg}'>{cells}</tr>"

    body_html = f"""
    <div style="padding:22px 32px;border-bottom:1px solid #eee">
      <table style="border-collapse:collapse">
        <tr><td style="color:#666;padding:4px 16px 4px 0">Region</td>
            <td style="font-weight:bold">{_esc(region)}</td></tr>
        <tr><td style="color:#666;padding:4px 16px 4px 0">Place of Utilization</td>
            <td style="font-weight:bold">{_esc(pou)}</td></tr>
        <tr><td style="color:#666;padding:4px 16px 4px 0">Course</td>
            <td style="font-weight:bold">{_esc(course)}</td></tr>
        <tr><td style="color:#666;padding:4px 16px 4px 0">Detected at</td>
            <td style="font-weight:bold">{_esc(timestamp)}</td></tr>
      </table>
    </div>
    <div style="padding:22px 32px">
      <h2 style="color:{_BRAND_COLOR};font-size:15px;margin:0 0 14px">Available Batch(es)</h2>
      <div style="overflow-x:auto">
        <table style="border-collapse:collapse;width:100%;font-size:14px">
          {table_rows}
        </table>
      </div>
    </div>"""

    html_body = _wrap_html(
        title      = "ICAI Batch Alert",
        subtitle   = "New or updated batch detected for your subscription",
        body_html  = body_html,
        cta_url    = "https://www.icaionlineregistration.org/launchbatchdetail.aspx",
        cta_label  = "Register on ICAI Portal →",
    )
    return plain, html_body


def send_itt_oc_alert(
    batches:    list[dict],
    region:     str,
    pou:        str,
    course:     str,
    to_email:   str | None = None,
):
    """
    Send an ITT/OC batch alert email.

    Args:
        batches   : list of batch dicts from scraper
        region    : e.g. "Western"
        pou       : e.g. "Pune"
        course    : e.g. "AICITSS - Advanced Information Technology"
        to_email  : recipient; falls back to ALERT_EMAIL env var if None
    """
    recipient = to_email or os.environ.get("ALERT_EMAIL") or os.environ.get("GMAIL_USER")
    if not recipient:
        logger.warning("[Email] send_itt_oc_alert: no recipient — set ALERT_EMAIL or pass to_email")
        return

    subject         = f"ICAI Batch Alert — {course} | {pou} ({region})"
    plain, html_body = _build_itt_oc_html(batches, region, pou, course)
    _send_email(recipient, subject, plain, html_body)


# ─── SPOM slot alert ──────────────────────────────────────────────────────────

def _build_spom_html(
    new_dates_by_centre: dict[str, list[str]],
    state_label:         str,
    city_label:          str,
) -> tuple[str, str]:
    """
    Build plain-text + HTML for a SPOM new-slot alert.

    new_dates_by_centre: { centre_name: ["04-May-2026", ...], ... }
    """
    timestamp = _now_ist()

    # Plain text
    lines = [
        "SPOM SLOT AVAILABILITY ALERT",
        "=" * 50,
        "New exam slots have opened for:",
        f"  State : {state_label}",
        f"  City  : {city_label}",
        f"  Checked: {timestamp}",
        "",
        "NEWLY AVAILABLE SLOTS:",
        "-" * 50,
    ]
    for centre, dates in new_dates_by_centre.items():
        lines.append(f"\nCentre: {centre}")
        for d in dates:
            lines.append(f"  ✅ {d}")
    lines += [
        "",
        "-" * 50,
        "Book your slot here: https://spmt.icai.org/ICAI/LoginAction_showSlotDetails.action",
        "",
        "— ICAI Batch Monitor (automated alert)",
    ]
    plain = "\n".join(lines)

    # HTML centre cards
    cards_html = ""
    for centre, dates in new_dates_by_centre.items():
        date_chips = "".join(
            f"<span style='display:inline-block;background:#e8f5e9;color:#1b5e20;"
            f"border-radius:4px;padding:4px 10px;margin:4px 4px 4px 0;"
            f"font-size:13px;font-weight:bold'>✅ {_esc(d)}</span>"
            for d in dates
        )
        cards_html += f"""
        <div style="border:1px solid #e0e0e0;border-radius:6px;padding:16px 20px;margin-bottom:16px">
          <h3 style="color:{_BRAND_COLOR};font-size:14px;margin:0 0 10px">
            🏛️ {_esc(centre)}
          </h3>
          <div>{date_chips}</div>
        </div>"""

    body_html = f"""
    <div style="padding:22px 32px;border-bottom:1px solid #eee">
      <table style="border-collapse:collapse">
        <tr><td style="color:#666;padding:4px 16px 4px 0">State</td>
            <td style="font-weight:bold">{_esc(state_label)}</td></tr>
        <tr><td style="color:#666;padding:4px 16px 4px 0">City</td>
            <td style="font-weight:bold">{_esc(city_label)}</td></tr>
        <tr><td style="color:#666;padding:4px 16px 4px 0">Detected at</td>
            <td style="font-weight:bold">{_esc(timestamp)}</td></tr>
      </table>
    </div>
    <div style="padding:22px 32px">
      <h2 style="color:{_BRAND_COLOR};font-size:15px;margin:0 0 16px">
        Newly Available Exam Slots
      </h2>
      {cards_html}
      <p style="color:#888;font-size:12px;margin:16px 0 0">
        Slots fill up quickly. Book at the earliest opportunity.
      </p>
    </div>"""

    html_body = _wrap_html(
        title     = "SPOM Slot Alert",
        subtitle  = f"New exam slot(s) opened in {city_label}, {state_label}",
        body_html = body_html,
        cta_url   = "https://spmt.icai.org/ICAI/LoginAction_showSlotDetails.action",
        cta_label = "Book Your Slot Now →",
    )
    return plain, html_body


def send_spom_alert(
    new_dates_by_centre: dict[str, list[str]],
    state_label:         str,
    city_label:          str,
    to_email:            str,
):
    """
    Send a SPOM slot availability alert email.

    Args:
        new_dates_by_centre : { centre_name: [date_str, ...], ... }
        state_label         : human-readable state name
        city_label          : human-readable city name
        to_email            : recipient email address
    """
    if not new_dates_by_centre:
        return

    total_dates = sum(len(v) for v in new_dates_by_centre.values())
    centres     = len(new_dates_by_centre)
    subject     = (
        f"SPOM Slot Alert — {total_dates} new slot(s) in {city_label}, {state_label} "
        f"({centres} centre(s))"
    )
    plain, html_body = _build_spom_html(new_dates_by_centre, state_label, city_label)
    _send_email(to_email, subject, plain, html_body)


# ─── Test helpers ─────────────────────────────────────────────────────────────

def send_test_email(to_email: str | None = None):
    """Send a test ITT/OC email to verify credentials are working."""
    dummy_batches = [{
        "Batch No":        "TEST-001",
        "Start Date":      "01 May 2026",
        "End Date":        "05 May 2026",
        "Venue":           "Pune ICAI Office",
        "Available Seats": "40",
        "Status":          "Open",
    }]
    recipient = to_email or os.environ.get("ALERT_EMAIL") or os.environ.get("GMAIL_USER")
    if not recipient:
        logger.error("send_test_email: no recipient configured.")
        return
    send_itt_oc_alert(
        dummy_batches,
        region   = "Western",
        pou      = "Pune",
        course   = "AICITSS - Advanced Information Technology",
        to_email = recipient,
    )


def send_test_spom_email(to_email: str | None = None):
    """Send a test SPOM email to verify credentials are working."""
    dummy_new_dates = {
        "Dexit Global Limited - Pune": ["04-May-2026", "05-May-2026"],
        "Nova Consultancy Services":   ["06-May-2026"],
    }
    recipient = to_email or os.environ.get("ALERT_EMAIL") or os.environ.get("GMAIL_USER")
    if not recipient:
        logger.error("send_test_spom_email: no recipient configured.")
        return
    send_spom_alert(
        dummy_new_dates,
        state_label = "Maharashtra",
        city_label  = "Pune",
        to_email    = recipient,
    )
