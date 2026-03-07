"""
alerts/alert_manager.py
=======================
Sends alerts when the FAIL count exceeds a configured threshold.

Supports:
  1. Slack  — via Incoming Webhook URL
  2. Email  — via SMTP (works with Gmail, Outlook, custom SMTP)

Configuration is read from the YAML rules file:

    alerts:
      fail_threshold: 5        # send alert if more than 5 FAILs
      slack:
        enabled: true
        webhook_url: "https://hooks.slack.com/services/..."
      email:
        enabled: true
        smtp_host: "smtp.gmail.com"
        smtp_port: 587
        sender:   "your_email@gmail.com"
        password: "your_app_password"    # use env var in production!
        recipients: ["team@company.com"]

SECURITY NOTE:
  Never hardcode passwords in YAML.
  Set: export DQ_EMAIL_PASSWORD="yourpassword"
  The code reads it from the environment variable automatically.
"""

from __future__ import annotations

import json
import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List
from urllib.request import Request, urlopen
from urllib.error import URLError

from core.base import CheckResult
from core.exceptions import DQAlertError


log = logging.getLogger("AlertManager")


# ==============================================================================
# SlackAlerter
# ==============================================================================

class SlackAlerter:
    """
    Sends a formatted alert message to a Slack channel
    via an Incoming Webhook URL.

    Setup in Slack:
      1. Go to https://api.slack.com/apps
      2. Create App → Incoming Webhooks → Activate → Add Webhook to Workspace
      3. Copy the webhook URL into your YAML config
    """

    def __init__(self, webhook_url: str) -> None:
        self.webhook_url = webhook_url

    def send(self, dataset_name: str, fail_count: int,
             results: List[CheckResult]) -> None:
        """Sends a Slack message with a summary of failures."""
        fail_lines = [
            f"• *{r.check_name}* on `{r.column}` — {r.violations:,} violations"
            for r in results if r.status == "FAIL"
        ][:10]  # max 10 lines to keep message readable

        message = {
            "text": (
                f":red_circle: *ETL DQ ALERT — {dataset_name}*\n"
                f"*{fail_count} checks FAILED*\n\n"
                + "\n".join(fail_lines)
                + ("\n_...and more_" if fail_count > 10 else "")
            )
        }

        try:
            data    = json.dumps(message).encode("utf-8")
            request = Request(self.webhook_url, data=data,
                              headers={"Content-Type": "application/json"})
            with urlopen(request, timeout=10) as resp:
                if resp.status != 200:
                    raise DQAlertError(f"Slack returned status {resp.status}")
            log.info(f"Slack alert sent for {dataset_name} | fails={fail_count}")
        except URLError as exc:
            raise DQAlertError(f"Slack alert failed: {exc}") from exc


# ==============================================================================
# EmailAlerter
# ==============================================================================

class EmailAlerter:
    """
    Sends an HTML email alert via SMTP.

    Works with:
      - Gmail (use App Password, not your main password)
      - Outlook
      - Any SMTP server

    For Gmail:
      1. Enable 2-step verification on your Google account
      2. Go to: Google Account → Security → App Passwords
      3. Create an App Password for "Mail"
      4. Use that 16-character password here (or in env var)
    """

    def __init__(self, smtp_host: str, smtp_port: int,
                 sender: str, password: str, recipients: list[str]) -> None:
        self.smtp_host  = smtp_host
        self.smtp_port  = smtp_port
        self.sender     = sender
        # Read password from environment variable if not provided directly
        # This keeps secrets out of config files
        self.password   = password or os.environ.get("DQ_EMAIL_PASSWORD", "")
        self.recipients = recipients

    def send(self, dataset_name: str, fail_count: int,
             results: List[CheckResult]) -> None:
        """Sends an HTML email with the DQ failure summary."""
        subject = f"[DQ ALERT] {fail_count} checks FAILED — {dataset_name}"

        # Build HTML body
        fail_rows = "".join(
            f"<tr style='background:#2a0a0a'>"
            f"<td style='padding:8px;border-bottom:1px solid #3a1a1a'>{r.check_name}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #3a1a1a'><code>{r.column}</code></td>"
            f"<td style='padding:8px;border-bottom:1px solid #3a1a1a;color:#ef4444'>{r.violations:,}</td>"
            f"</tr>"
            for r in results if r.status == "FAIL"
        )[:15]  # max 15 rows

        html_body = f"""
        <html><body style="font-family:Arial;background:#0f1117;color:#e2e8f0;padding:24px">
        <h2 style="color:#ef4444">🔴 ETL DQ Alert — {dataset_name}</h2>
        <p><strong>{fail_count} checks FAILED</strong></p>
        <table style="width:100%;border-collapse:collapse;margin-top:16px">
          <thead>
            <tr style="background:#1a1d27">
              <th style="padding:10px;text-align:left;color:#8892a4">Check</th>
              <th style="padding:10px;text-align:left;color:#8892a4">Column</th>
              <th style="padding:10px;text-align:left;color:#8892a4">Violations</th>
            </tr>
          </thead>
          <tbody>{fail_rows}</tbody>
        </table>
        <p style="margin-top:20px;color:#8892a4;font-size:12px">
          ETL DQ Framework v6 — automated alert
        </p>
        </body></html>
        """

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = self.sender
        msg["To"]      = ", ".join(self.recipients)
        msg.attach(MIMEText(html_body, "html"))

        try:
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as smtp:
                smtp.ehlo()
                smtp.starttls()
                smtp.login(self.sender, self.password)
                smtp.sendmail(self.sender, self.recipients, msg.as_string())
            log.info(f"Email alert sent | to={self.recipients} | fails={fail_count}")
        except smtplib.SMTPException as exc:
            raise DQAlertError(f"Email alert failed: {exc}") from exc


# ==============================================================================
# AlertManager — coordinates Slack + Email from YAML config
# ==============================================================================

class AlertManager:
    """
    Reads alert configuration from YAML and fires alerts when
    the fail_count exceeds the threshold.

    Usage
    -----
    alert_manager = AlertManager(alert_config)
    alert_manager.fire_if_needed("employees", runner.fail_count, runner.all_results)
    """

    def __init__(self, alert_config: dict) -> None:
        self.config    = alert_config
        self.threshold = alert_config.get("fail_threshold", 5)

    def fire_if_needed(
        self,
        dataset_name : str,
        fail_count   : int,
        results      : List[CheckResult],
    ) -> None:
        """
        Checks if fail_count > threshold.
        If yes, sends Slack and/or email alerts as configured.
        """
        if fail_count <= self.threshold:
            log.info(
                f"Alert check: {fail_count} FAILs ≤ threshold {self.threshold} — no alert sent"
            )
            return

        log.warning(
            f"ALERT TRIGGERED | dataset={dataset_name} | "
            f"fails={fail_count} > threshold={self.threshold}"
        )
        print(f"\n  ⚠  ALERT: {fail_count} FAILs exceeded threshold of {self.threshold}")

        # ── Slack ─────────────────────────────────────────────────────────────
        slack_cfg = self.config.get("slack", {})
        if slack_cfg.get("enabled") and slack_cfg.get("webhook_url"):
            try:
                SlackAlerter(slack_cfg["webhook_url"]).send(
                    dataset_name, fail_count, results
                )
                print("  ✓ Slack alert sent")
            except DQAlertError as exc:
                print(f"  ✗ Slack alert failed: {exc}")
                log.error(f"Slack alert failed: {exc}")

        # ── Email ─────────────────────────────────────────────────────────────
        email_cfg = self.config.get("email", {})
        if email_cfg.get("enabled") and email_cfg.get("recipients"):
            try:
                EmailAlerter(
                    smtp_host  = email_cfg.get("smtp_host", "smtp.gmail.com"),
                    smtp_port  = email_cfg.get("smtp_port", 587),
                    sender     = email_cfg.get("sender", ""),
                    password   = email_cfg.get("password", ""),
                    recipients = email_cfg.get("recipients", []),
                ).send(dataset_name, fail_count, results)
                print("  ✓ Email alert sent")
            except DQAlertError as exc:
                print(f"  ✗ Email alert failed: {exc}")
                log.error(f"Email alert failed: {exc}")
