"""
notifier.py
-----------
Sends email alerts via Gmail SMTP when new ICAI batches are detected.

Prerequisites (set as GitHub Secrets):
  GMAIL_USER     → your Gmail address (e.g. yourname@gmail.com)
  GMAIL_APP_PASS → 16-char App Password (NOT your Gmail login password)
  ALERT_EMAIL    → where to send alerts (can be same as GMAIL_USER)

To create a Gmail App Password:
  1. Enable 2-Factor Authentication on your Google account
  2. Go to: Google Account → Security → App Passwords
  3. Generate one for "Mail" → "Windows Computer" (label doesn't matter)
  4. Copy the 16-char code into your GitHub Secret
"""

import smtplib
import os
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

logger = logging.getLogger(__name__)


def _build_email_body(batches: list[dict], region: str, pou: str, course: str) -> tuple[str, str]:
    """Build plain-text and HTML versions of the alert email."""

    timestamp = datetime.now().strftime("%d %b %Y, %I:%M %p")

    # ── Plain text ─────────────────────────────────────────────────────────────
    lines = [
        "🚨 ICAI BATCH ALERT",
        "=" * 50,
        f"New/updated batch(es) detected for your preferences:",
        f"  Region : {region}",
        f"  PoU    : {pou}",
        f"  Course : {course}",
        f"  Checked: {timestamp}",
        "",
        "BATCH DETAILS:",
        "-" * 50,
    ]

    for i, batch in enumerate(batches, 1):
        lines.append(f"\nBatch #{i}")
        for key, val in batch.items():
            lines.append(f"  {key}: {val}")

    lines += [
        "",
        "-" * 50,
        "Register here: https://www.icaionlineregistration.org/launchbatchdetail.aspx",
        "",
        "— ICAI Batch Monitor (automated alert)",
    ]

    plain = "\n".join(lines)

    # ── HTML ───────────────────────────────────────────────────────────────────
    table_rows = ""
    if batches:
        headers = list(batches[0].keys())
        header_cells = "".join(f"<th style='padding:8px 12px;background:#1a3c6e;color:#fff;text-align:left'>{h}</th>" for h in headers)
        table_rows += f"<tr>{header_cells}</tr>"
        for i, batch in enumerate(batches):
            bg = "#f0f4ff" if i % 2 == 0 else "#ffffff"
            cells = "".join(
                f"<td style='padding:8px 12px;border-bottom:1px solid #e0e0e0'>{batch.get(h, '')}</td>"
                for h in headers
            )
            table_rows += f"<tr style='background:{bg}'>{cells}</tr>"

    html = f"""
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="font-family:Arial,sans-serif;margin:0;padding:0;background:#f5f5f5">
  <div style="max-width:680px;margin:30px auto;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.1)">

    <!-- Header -->
    <div style="background:#1a3c6e;padding:24px 32px">
      <h1 style="color:#fff;margin:0;font-size:22px">🚨 ICAI Batch Alert</h1>
      <p style="color:#a8c4e8;margin:6px 0 0">New or updated batch detected</p>
    </div>

    <!-- Preferences summary -->
    <div style="padding:24px 32px;border-bottom:1px solid #eee">
      <h2 style="color:#1a3c6e;font-size:16px;margin:0 0 12px">Your Preferences</h2>
      <table style="border-collapse:collapse">
        <tr><td style="color:#666;padding:4px 16px 4px 0">Region</td><td style="font-weight:bold">{region}</td></tr>
        <tr><td style="color:#666;padding:4px 16px 4px 0">Place of Utilization</td><td style="font-weight:bold">{pou}</td></tr>
        <tr><td style="color:#666;padding:4px 16px 4px 0">Course</td><td style="font-weight:bold">{course}</td></tr>
        <tr><td style="color:#666;padding:4px 16px 4px 0">Detected at</td><td style="font-weight:bold">{timestamp}</td></tr>
      </table>
    </div>

    <!-- Batch table -->
    <div style="padding:24px 32px">
      <h2 style="color:#1a3c6e;font-size:16px;margin:0 0 16px">Available Batch(es)</h2>
      <div style="overflow-x:auto">
        <table style="border-collapse:collapse;width:100%;font-size:14px">
          {table_rows}
        </table>
      </div>
    </div>

    <!-- CTA -->
    <div style="padding:0 32px 32px;text-align:center">
      <a href="https://www.icaionlineregistration.org/launchbatchdetail.aspx"
         style="display:inline-block;background:#1a3c6e;color:#fff;padding:12px 28px;border-radius:6px;text-decoration:none;font-weight:bold;font-size:15px">
        Register on ICAI Portal →
      </a>
      <p style="color:#999;font-size:12px;margin:16px 0 0">
        Seats fill up fast. Register at the earliest opportunity.
      </p>
    </div>

    <!-- Footer -->
    <div style="background:#f5f5f5;padding:16px 32px;text-align:center">
      <p style="color:#aaa;font-size:12px;margin:0">
        ICAI Batch Monitor · Automated alert · Checked every 10 minutes
      </p>
    </div>

  </div>
</body>
</html>
"""
    return plain, html


def send_alert(batches: list[dict], region: str, pou: str, course: str):
    """
    Send an email alert via Gmail SMTP.
    Reads credentials from environment variables (set as GitHub Secrets).
    """
    gmail_user = os.environ.get("GMAIL_USER")
    gmail_pass = os.environ.get("GMAIL_APP_PASS")
    alert_email = os.environ.get("ALERT_EMAIL", gmail_user)

    if not gmail_user or not gmail_pass:
        raise EnvironmentError(
            "Missing GMAIL_USER or GMAIL_APP_PASS environment variables. "
            "Set them as GitHub Secrets."
        )

    subject = f"🚨 ICAI Batch Alert — {course} | {pou} ({region})"
    plain, html = _build_email_body(batches, region, pou, course)

    # Build MIME email with both plain and HTML parts
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"ICAI Monitor <{gmail_user}>"
    msg["To"] = alert_email
    msg.attach(MIMEText(plain, "plain"))
    msg.attach(MIMEText(html, "html"))

    logger.info(f"Sending alert email to {alert_email}...")

    with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as server:
        server.login(gmail_user, gmail_pass)
        server.sendmail(gmail_user, alert_email, msg.as_string())

    logger.info("✅ Alert email sent successfully")


def send_test_email():
    """Send a test email to verify credentials are working (used during setup)."""
    dummy_batches = [
        {
            "Batch No": "TEST-001",
            "Start Date": "01 May 2026",
            "End Date": "05 May 2026",
            "Venue": "Pune ICAI Office",
            "Available Seats": "40",
            "Status": "Open",
        }
    ]
    send_alert(dummy_batches, region="Western", pou="Pune",
               course="AICITSS - Advanced Information Technology")
