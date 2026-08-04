"""
Databricks App boilerplate:
- Serves a small Flask API
- Reads/writes to Lakebase (Databricks-managed Postgres) via lakebase.py
- Pulls data from the Massive API via massive_client.py and syncs it into Lakebase

Run locally:
    python app.py
Deploy as a Databricks App using app.yaml.
"""

import logging
import os
import re
from datetime import datetime, timezone

import requests
from databricks.sdk import WorkspaceClient
from flask import Flask, jsonify, render_template, request, redirect, url_for

import lakebase
# from massive_client import MassiveClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("massive-app")

app = Flask(__name__)
_w = WorkspaceClient()

TICKETS_TABLE_NAME = os.environ.get("TICKETS_TABLE_NAME", "tickets")
TICKET_MESSAGES_TABLE_NAME = os.environ.get("TICKET_MESSAGES_TABLE_NAME", "ticket_messages")

# Basic stock ticker shape check: 1-10 uppercase letters, with an optional
# ".X" or ".XX" share-class suffix (e.g. "BRK.B"). This rejects obviously
# malformed input before we even call the Massive API.
# _TICKER_RE = re.compile(r"^[A-Z]{1,10}(\.[A-Z]{1,2})?$")


def ensure_tickets_table():
    """Create the tickets table in Lakebase if it doesn't exist yet."""
    lakebase.run_write(
        f"""
        CREATE TABLE IF NOT EXISTS {TICKETS_TABLE_NAME} (
            ticket_id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            status TEXT NOT NULL,
            priority INTEGER NOT NULL DEFAULT 3,
            created_by TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    
    # Add priority column if it doesn't exist (for existing tables)
    try:
        lakebase.run_write(
            f"""
            ALTER TABLE {TICKETS_TABLE_NAME} 
            ADD COLUMN IF NOT EXISTS priority INTEGER NOT NULL DEFAULT 3
            """
        )
    except Exception:
        pass  # Column might already exist or ALTER not supported


def ensure_ticket_messages_table():
    """Create the ticket_messages table in Lakebase if it doesn't exist yet."""
    lakebase.run_write(
        f"""
        CREATE TABLE IF NOT EXISTS {TICKET_MESSAGES_TABLE_NAME} (
            message_id SERIAL PRIMARY KEY,
            ticket_id INTEGER NOT NULL REFERENCES {TICKETS_TABLE_NAME}(ticket_id),
            message_text TEXT NOT NULL,
            author TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )


def _current_user_email() -> str:
    """
    Resolve the current user's email so the watchlist can be personalized.

    Databricks Apps inject the logged-in user's identity via the
    X-Forwarded-Email header on every request. Fall back to the Databricks
    SDK's current_user API for local development where that header isn't set.
    """
    header_email = request.headers.get("X-Forwarded-Email")
    if header_email:
        return header_email
    return _w.current_user.me().user_name


def format_ticket_age(created_at):
    """Format ticket age in human-readable format based on duration.
    
    Args:
        created_at: datetime object (timezone-aware)
    
    Returns:
        String with formatted age
    """
    # Ensure created_at is timezone-aware
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    
    now = datetime.now(timezone.utc)
    delta = now - created_at
    
    total_seconds = int(delta.total_seconds())
    minutes = total_seconds // 60
    hours = total_seconds // 3600
    days = delta.days
    
    # Under 60 minutes: show in minutes
    if minutes < 60:
        return f"{minutes}m"
    
    # Under 24 hours: show in hours and minutes
    elif hours < 24:
        remaining_minutes = minutes % 60
        return f"{hours}h {remaining_minutes}m"
    
    # Under 7 days: show in days and hours
    elif days < 7:
        remaining_hours = hours % 24
        return f"{days}d {remaining_hours}h"
    
    # Under a month (30 days): show in weeks, days, and hours
    elif days < 30:
        weeks = days // 7
        remaining_days = days % 7
        remaining_hours = hours % 24
        return f"{weeks}w {remaining_days}d {remaining_hours}h"
    
    # A year or more: show in years, months, days, and hours
    else:
        years = days // 365
        remaining_days_after_years = days % 365
        months = remaining_days_after_years // 30
        remaining_days_after_months = remaining_days_after_years % 30
        remaining_hours = hours % 24
        
        if years > 0:
            return f"{years}y {months}mo {remaining_days_after_months}d {remaining_hours}h"
        else:
            # Just months (between 30 days and 1 year)
            return f"{months}mo {remaining_days_after_months}d {remaining_hours}h"


@app.route("/healthz")
def healthz():
    return jsonify({"status": "ok"})


@app.errorhandler(Exception)
def handle_exception(err):
    """Ensure all unhandled errors return JSON (not an HTML error page),
    so the frontend's resp.json() call never chokes on HTML."""
    logger.exception("Unhandled exception while processing request")
    status_code = getattr(err, "code", 500)
    if not isinstance(status_code, int):
        status_code = 500
    return jsonify({"error": str(err)}), status_code


@app.route("/")
def index():
    """Redirect to the tickets list page."""
    return redirect(url_for('list_tickets'))


@app.route("/tickets", methods=["GET"])
def list_tickets():
    """List all tickets."""
    ensure_tickets_table()
    
    tickets = lakebase.run_query(
        f"""
        SELECT ticket_id, title, status, priority, created_by, created_at
        FROM {TICKETS_TABLE_NAME}
        ORDER BY priority ASC, created_at DESC
        """
    )
    
    # Add formatted age to each ticket
    for ticket in tickets:
        ticket['age'] = format_ticket_age(ticket['created_at'])
    
    return render_template("tickets_list.html", tickets=tickets)


@app.route("/tickets/create", methods=["GET", "POST"])
def create_ticket():
    """Create a new ticket."""
    if request.method == "GET":
        return render_template("create_ticket.html")
    
    # POST request - create the ticket
    ensure_tickets_table()
    
    title = request.form.get("title", "").strip()
    priority = request.form.get("priority", "3").strip()
    
    # Validation
    if not title:
        return render_template("create_ticket.html", error="Title is required"), 400
    
    # Validate priority is a valid integer between 1 and 5
    try:
        priority_int = int(priority)
        if priority_int < 1 or priority_int > 5:
            return render_template("create_ticket.html", error="Priority must be between 1 and 5"), 400
    except ValueError:
        return render_template("create_ticket.html", error="Invalid priority value"), 400
    
    # Get current user email
    email = _current_user_email()
    
    # Insert ticket into database with default status "pending"
    lakebase.run_write(
        f"""
        INSERT INTO {TICKETS_TABLE_NAME} (title, status, priority, created_by, created_at)
        VALUES (%s, %s, %s, %s, now())
        """,
        (title, "pending", priority_int, email)
    )
    
    # Redirect to tickets list page after successful creation
    return redirect(url_for('list_tickets'))


@app.route("/tickets/<int:ticket_id>", methods=["GET"])
def view_ticket(ticket_id):
    """View a single ticket with all its messages."""
    ensure_tickets_table()
    ensure_ticket_messages_table()
    
    # Get ticket details
    ticket_result = lakebase.run_query(
        f"""
        SELECT ticket_id, title, status, priority, created_by, created_at
        FROM {TICKETS_TABLE_NAME}
        WHERE ticket_id = %s
        """,
        (ticket_id,)
    )
    
    if not ticket_result:
        return "Ticket not found", 404
    
    ticket = ticket_result[0]
    
    # Get all messages for this ticket
    messages = lakebase.run_query(
        f"""
        SELECT message_id, message_text, author, created_at
        FROM {TICKET_MESSAGES_TABLE_NAME}
        WHERE ticket_id = %s
        ORDER BY created_at ASC
        """,
        (ticket_id,)
    )
    
    return render_template("ticket_detail.html", ticket=ticket, messages=messages)


@app.route("/tickets/<int:ticket_id>/messages", methods=["POST"])
def add_message(ticket_id):
    """Add a message to a ticket."""
    ensure_ticket_messages_table()
    
    message_text = request.form.get("message_text", "").strip()
    
    # Validation
    if not message_text:
        # Redirect back with error - for simplicity, we'll just redirect
        return redirect(url_for('view_ticket', ticket_id=ticket_id))
    
    # Get current user email
    email = _current_user_email()
    
    # Insert message into database
    lakebase.run_write(
        f"""
        INSERT INTO {TICKET_MESSAGES_TABLE_NAME} (ticket_id, message_text, author, created_at)
        VALUES (%s, %s, %s, now())
        """,
        (ticket_id, message_text, email)
    )
    
    # Redirect back to ticket detail page
    return redirect(url_for('view_ticket', ticket_id=ticket_id))


@app.route("/tickets/<int:ticket_id>/status", methods=["POST"])
def update_ticket_status(ticket_id):
    """Update the status of a ticket."""
    ensure_tickets_table()
    
    new_status = request.form.get("status", "").strip()
    
    # Validate status is one of the allowed values
    allowed_statuses = ["pending", "open", "in_progress", "resolved", "closed"]
    if new_status not in allowed_statuses:
        # Invalid status, just redirect back
        return redirect(url_for('view_ticket', ticket_id=ticket_id))
    
    # Update ticket status in database
    lakebase.run_write(
        f"""
        UPDATE {TICKETS_TABLE_NAME}
        SET status = %s
        WHERE ticket_id = %s
        """,
        (new_status, ticket_id)
    )
    
    # Redirect back to ticket detail page
    return redirect(url_for('view_ticket', ticket_id=ticket_id))


if __name__ == '__main__':
    host = os.getenv('FLASK_RUN_HOST', '0.0.0.0')
    port = int(os.getenv('FLASK_RUN_PORT', 8000))
    app.run(debug=True, host=host, port=port)
    print(f"Flask app running on http://{host}:{port}")
