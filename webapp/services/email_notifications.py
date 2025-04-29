import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import os
from dotenv import load_dotenv
from dateutil.parser import parse

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Email configuration from .env
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")
EMAIL_HOST = os.getenv("EMAIL_HOST")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", 465))
EMAIL_SECURE = os.getenv("EMAIL_SECURE", "true").lower() == "true"
EMAIL_REPLYTO = os.getenv("EMAIL_REPLYTO")

# Default recipient email from .env
DEFAULT_RECIPIENT_EMAIL = os.getenv("DEFAULT_RECIPIENT_EMAIL")

def get_ordinal_suffix(day):
    """Returns the ordinal suffix for a given day (e.g., 'st', 'nd', 'rd', 'th')."""
    if 10 <= day % 100 <= 20:
        suffix = 'th'
    else:
        suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(day % 10, 'th')
    return suffix

def format_datetime_readable(dt_str):
    """
    Converts a datetime string (or uses current time if None) to a readable format.
    Example: 'Monday, 28th April, 2025 at 10:00am'
    """
    logger.debug(f"Attempting to parse datetime: {dt_str}")
    try:
        if isinstance(dt_str, datetime):
            # If it's already a datetime object, use it directly
            dt = dt_str
        elif dt_str:
            # Try parsing with dateutil.parser if it's a string
            dt = parse(dt_str, fuzzy=True)
        else:
            dt = datetime.now()
    except (ValueError, TypeError) as e:
        logger.warning(f"Invalid datetime format for {dt_str}, using current time instead. Error: {e}")
        dt = datetime.now()

    # Get the day with ordinal suffix (e.g., "28th")
    day = dt.day
    ordinal_suffix = get_ordinal_suffix(day)

    # Format the datetime to "Monday, 28th April, 2025 at 10:00am"
    hour_minute_am_pm = dt.strftime("%I").lstrip('0') + dt.strftime(":%M%p").lower()
    formatted_dt = dt.strftime(f"%A, {day}{ordinal_suffix} %B, %Y at {hour_minute_am_pm}")
    return formatted_dt

def send_open_tender_email(tender_data, recipient_email=None):
    """
    Sends an email notification for an open tender to the specified recipient.
    
    Args:
        tender_data (dict): The tender data containing details like title, source_url, etc.
        recipient_email (str, optional): The email address to send the notification to. If None, the default from .env will be used.
    """
    try:
        # If no recipient email is passed, use the default from the environment variable
        if recipient_email is None:
            recipient_email = DEFAULT_RECIPIENT_EMAIL

        # Create the email message
        msg = MIMEMultipart()
        msg["From"] = EMAIL_USER
        msg["To"] = recipient_email
        msg["Subject"] = f"New Open Tender: {tender_data.get('title', 'N/A')}"
        msg["Reply-To"] = EMAIL_REPLYTO

        # Format the scraped_at timestamp
        scraped_at_formatted = format_datetime_readable(tender_data.get('scraped_at'))

        # Email body with improved formatting
        body = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>
            body {{
                font-family: 'Inter', 'Arial', sans-serif;
                background-color: #f4f4f4;
                margin: 0;
                padding: 0;
            }}
            .container {{
                max-width: 600px;
                margin: 20px auto;
                background-color: #ffffff;
                border-radius: 8px;
                box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
                overflow: hidden;
            }}
            .header {{
                background-color: #f4f4f4;
                padding: 30px;
                text-align: center;
            }}
            .header img {{
                max-width: 150px;
                height: auto;
                padding: 10px;
            }}
            .content {{
                padding: 30px;
            }}
            h2 {{
                color: #e04c1b;
                font-size: 28px;
                margin-bottom: 15px;
                text-align: center;
            }}
            p {{
                color: #333333;
                font-size: 16px;
                line-height: 1.6;
                margin-bottom: 20px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-bottom: 20px;
                aria-label: "Tender Details";
            }}
            th, td {{
                padding: 12px;
                text-align: left;
                font-size: 15px;
                border-bottom: 1px solid #e0e0e0;
            }}
            th {{
                background-color: #f9f9f9;
                color: #1a1a1a;
                width: 30%;
            }}
            td {{
                color: #333333;
            }}
            tr:nth-child(even) {{
                background-color: #fafafa;
            }}
            tr:hover {{
                background-color: #f5f5f5;
            }}
            .btn {{
                display: inline-block;
                padding: 8px 16px;
                background-color: #e04c1b;
                color: #fff;
                border-radius: 4px;
                text-decoration: none;
                font-weight: 600;
            }}
            .btn:hover {{
                background-color: #c43e17;
            }}
            .btn:focus {{
                outline: 2px solid #e04c1b;
                outline-offset: 2px;
            }}
            a {{
                color: #1a1a1a;
                text-decoration: none;
            }}
            a:hover {{
                text-decoration: underline;
            }}
            .footer {{
                background-color: #f4f4f4;
                padding: 20px;
                text-align: center;
                font-size: 14px;
                color: #666666;
            }}
            .footer a {{
                color: #1a1a1a;
                margin: 0 10px;
            }}
            @media only screen and (max-width: 600px) {{
                .container {{
                    margin: 10px;
                    padding: 15px;
                }}
                h2 {{
                    font-size: 20px;
                }}
                p, th, td {{
                    font-size: 14px;
                }}
                .header img {{
                    max-width: 120px;
                }}
                table, tr, th, td {{
                    display: block;
                    width: 100%;
                }}
                th {{
                    background-color: #e04c1b;
                    color: #fff;
                }}
                td {{
                    padding-left: 10px;
                }}
            }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <img src="https://growthpad.co.ke/wp-content/uploads/2024/10/GCG-final-logo-proposals_v6-6.png" alt="Growthpad Logo">
                </div>
                <div class="content">
                    <h2 style="text-align: center; color: #f05d23; font-size: 24px; margin-bottom: 10px;">New Open Tender Notification</h2>
                    <p>Hello GCG Business Department, A new open tender has been found that matches your criteria:</p>
                    <table>
                        <tr>
                            <th>Title</th>
                            <td>{tender_data.get('title', 'N/A')}</td>
                        </tr>
                        <tr>
                            <th>Reference Number</th>
                            <td>{tender_data.get('description', 'N/A')}</td>
                        </tr>
                        <tr>
                            <th>Closing Date</th>
                            <td>{tender_data.get('closing_date', 'N/A')}</td>
                        </tr>
                        <tr>
                            <th>Status</th>
                            <td>{tender_data.get('status', 'N/A')}</td>
                        </tr>
                        <tr>
                            <th>Source URL</th>
                            <td><a href="{tender_data.get('source_url', '#')}">{tender_data.get('source_url', 'N/A')}</a></td>
                        </tr>
                        <tr>
                            <th>Format</th>
                            <td>{tender_data.get('format', 'N/A')}</td>
                        </tr>
                        <tr>
                            <th>Tender Type</th>
                            <td>{tender_data.get('tender_type', 'N/A')}</td>
                        </tr>
                        <tr>
                            <th>Location</th>
                            <td>{tender_data.get('location', 'N/A')}</td>
                        </tr>
                    </table>
                    <p style="text-align: center;"><strong>Scraped on:</strong> {scraped_at_formatted}</p>
                </div>
                <div class="footer">
                    <p>© {datetime.now().year} Growthpad. All rights reserved.</p>
                    <p><a href="https://growthpad.co.ke">growthpad.co.ke</a> | <a href="mailto:strategic@growthpad.co.ke">Contact Us</a></p>
                </div>
            </div>
        </body>
        </html>
        """

        msg.attach(MIMEText(body, "html"))

        # Set up the SMTP server
        if EMAIL_SECURE:
            server = smtplib.SMTP_SSL(EMAIL_HOST, EMAIL_PORT)
        else:
            server = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT)
            server.starttls()

        # Login to the email server
        server.login(EMAIL_USER, EMAIL_PASS)

        # Send the email
        server.sendmail(EMAIL_USER, recipient_email, msg.as_string())
        logger.info(f"Email sent successfully to {recipient_email} for tender: {tender_data.get('title', 'N/A')}")

        # Close the server connection
        server.quit()

    except Exception as e:
        logger.error(f"Failed to send email for tender '{tender_data.get('title', 'N/A')}': {str(e)}")

def notify_open_tenders(tenders, task_id, recipient_email=None):
    """
    Checks a list of tenders and sends an email for each open tender.
    
    Args:
        tenders (list): List of tender dictionaries.
        task_id (int): The ID of the task that triggered this notification.
        recipient_email (str): The email address to send notifications to. Defaults to the value in .env.
    """
    for tender in tenders:
        if tender.get("status") == "open":
            logger.info(f"Found open tender for task {task_id}: {tender.get('title', 'N/A')}")
            send_open_tender_email(tender, recipient_email)