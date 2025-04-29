import smtplib
import logging
import re
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
            dt = dt_str
        elif dt_str:
            # Check if the string is a date-only string (length 10, format: 'YYYY-MM-DD')
            if len(dt_str) == 10:  # Format like '2025-04-29'
                current_time = datetime.now().strftime("%H:%M:%S")  # Get current time as HH:MM:SS
                dt_str = f"{dt_str} {current_time}"  # Append current time to the date string
                logger.debug(f"Date-only detected. Modified datetime string: {dt_str}")
            dt = parse(dt_str, fuzzy=True)  # Parse the full datetime string
        else:
            logger.warning(f"Empty or invalid datetime received. Using current time.")
            dt = datetime.now()  # Fall back to current time if none provided
    except (ValueError, TypeError) as e:
        logger.warning(f"Invalid datetime format for {dt_str}, using current time instead. Error: {e}")
        dt = datetime.now()

    day = dt.day
    ordinal_suffix = get_ordinal_suffix(day)
    hour_minute_am_pm = dt.strftime("%I").lstrip('0') + dt.strftime(":%M%p").lower()
    formatted_dt = dt.strftime(f"%A, {day}{ordinal_suffix} %B, %Y at {hour_minute_am_pm}")
    return formatted_dt

def validate_email(email):
    """Validates an email address using a regex pattern."""
    email_regex = r'^[^\s@]+@[^\s@]+\.[^\s@]+$'
    return bool(re.match(email_regex, email))

def send_open_tender_email(tender_data, recipient_email):
    """
    Sends an email notification for an open tender to the specified recipient.
    
    Args:
        tender_data (dict): The tender data containing details like title, source_url, etc.
        recipient_email (str): A single email address to send the notification to.
    
    Raises:
        ValueError: If the recipient_email is invalid.
    """
    if not validate_email(recipient_email):
        logger.error(f"Invalid email address: {recipient_email}")
        raise ValueError(f"Invalid email address: {recipient_email}")

    try:
        msg = MIMEMultipart()
        msg["From"] = EMAIL_USER
        msg["To"] = recipient_email
        msg["Subject"] = f"New Open Tender: {tender_data.get('title', 'N/A')}"
        msg["Reply-To"] = EMAIL_REPLYTO

        scraped_at_formatted = format_datetime_readable(tender_data.get('scraped_at'))
        current_year = datetime.now().year

        # Improved Email HTML Template
        body = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>
                @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
                
                body {{
                    font-family: 'Inter', Arial, sans-serif;
                    background-color: #f8f9fa;
                    margin: 0;
                    padding: 0;
                    color: #333333;
                    line-height: 1.6;
                }}
                
                .container {{
                    max-width: 650px;
                    margin: 25px auto;
                    background-color: #ffffff;
                    border-radius: 12px;
                    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08);
                    overflow: hidden;
                }}
                
                .header {{
                    background: linear-gradient(135deg, #f05d23 0%, #e04c1b 100%);
                    padding: 30px 20px;
                    text-align: center;
                }}
                
                .header img {{
                    max-width: 180px;
                    height: auto;
                    padding: 10px;
                    background-color: white;
                    border-radius: 8px;
                    box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
                }}
                
                .content {{
                    padding: 32px;
                }}
                
                .title {{
                    color: #f05d23;
                    font-size: 26px;
                    font-weight: 700;
                    margin-bottom: 20px;
                    text-align: center;
                    border-bottom: 2px solid #f8f9fa;
                    padding-bottom: 15px;
                }}
                
                .greeting {{
                    font-size: 17px;
                    margin-bottom: 20px;
                }}
                
                .table-wrapper {{
                    border-radius: 8px;
                    overflow: hidden;
                    border: 1px solid #e8eaed;
                    margin: 25px 0;
                }}
                
                table {{
                    width: 100%;
                    border-collapse: collapse;
                    aria-label: "Tender Details";
                }}
                
                th, td {{
                    padding: 14px 16px;
                    text-align: left;
                    font-size: 15px;
                }}
                
                th {{
                    background-color: #f05d23;
                    color: #ffffff;
                    font-weight: 600;
                    width: 35%;
                }}
                
                td {{
                    color: #333333;
                    text-transform: capitalize;
                    border-bottom: 1px solid #e8eaed;
                }}
                
                tr:last-child td {{
                    border-bottom: none;
                }}
                
                tr:nth-child(even) {{
                    background-color: #f8f9fa;
                }}
                
                .tender-url {{
                    color: #f05d23;
                    text-decoration: none;
                    font-weight: 500;
                    word-break: break-all;
                }}
                
                .tender-url:hover {{
                    text-decoration: underline;
                }}
                
                .status-badge {{
                    display: inline-block;
                    padding: 6px 12px;
                    background-color: #4caf50;
                    color: white;
                    border-radius: 50px;
                    font-size: 14px;
                    font-weight: 600;
                    text-transform: uppercase;
                }}
                
                .scraped-info {{
                    text-align: center;
                    margin-top: 28px;
                    padding: 14px;
                    background-color: #f8f9fa;
                    border-radius: 8px;
                    font-size: 15px;
                    color: #666;
                }}
                
                .btn-container {{
                    text-align: center;
                    margin-top: 30px;
                }}
                
                .btn {{
                    display: inline-block;
                    padding: 12px 28px;
                    background: linear-gradient(to right, #f05d23, #e04c1b);
                    color: #ffffff !important;
                    border-radius: 50px;
                    text-decoration: none;
                    font-weight: 600;
                    font-size: 16px;
                    transition: all 0.3s ease;
                    box-shadow: 0 4px 6px rgba(240, 93, 35, 0.2);
                }}
                
                .btn:hover {{
                    background: linear-gradient(to right, #e04c1b, #d13e15);
                    transform: translateY(-2px);
                    box-shadow: 0 6px 8px rgba(240, 93, 35, 0.3);
                }}
                
                .footer {{
                    background-color: #f8f9fa;
                    padding: 25px 20px;
                    text-align: center;
                    font-size: 14px;
                    color: #666666;
                    border-top: 1px solid #e8eaed;
                }}
                
                .footer a {{
                    color: #f05d23;
                    text-decoration: none;
                    margin: 0;
                }}
                
                .footer a:hover {{
                    text-decoration: underline;
                }}
                
                .social-links {{
                    margin: 15px 0;
                }}
                
                .social-icon {{
                    display: inline-block;
                    width: 32px;
                    height: 32px;
                    background-color: #f05d23;
                    border-radius: 50%;
                    margin: 0 5px;
                    text-align: center;
                    line-height: 32px;
                }}
                
                .social-icon a {{
                    color: white;
                    display: block;
                    width: 100%;
                    height: 100%;
                    border-radius: 50%;
                }}
                
                .social-icon a:hover {{
                    background-color: #e04c1b;
                }}
                
                @media only screen and (max-width: 600px) {{
                    .container {{
                        margin: 10px;
                        border-radius: 8px;
                    }}
                    
                    .content {{
                        padding: 20px;
                    }}
                    
                    .title {{
                        font-size: 22px;
                    }}
                    
                    .header img {{
                        max-width: 150px;
                    }}
                    
                    th, td {{
                        padding: 12px;
                        font-size: 14px;
                    }}
                    
                    th {{
                        padding-left: 12px;
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
                    <h2 class="title">New Open Tender Notification</h2>
                    <p class="greeting">Hello <strong>GCG Business Department</strong>,<br>A new open tender has been found that matches your criteria:</p>
                    
                    <div class="table-wrapper">
                        <table>
                            <tr>
                                <th>Title</th>
                                <td><strong>{tender_data.get('title', 'N/A')}</strong></td>
                            </tr>
                            <tr>
                                <th>Reference Number</th>
                                <td>{tender_data.get('description', 'N/A')}</td>
                            </tr>
                            <tr>
                                <th>Closing Date</th>
                                <td><strong>{tender_data.get('closing_date', 'N/A')}</strong></td>
                            </tr>
                            <tr>
                                <th>Status</th>
                                <td><span class="status-badge">{tender_data.get('status', 'N/A')}</span></td>
                            </tr>
                            <tr>
                                <th>Source URL</th>
                                <td><a class="tender-url" href="{tender_data.get('source_url', '#')}">{tender_data.get('source_url', 'N/A')}</a></td>
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
                    </div>
                    
                    <div class="scraped-info">
                        <strong>Scraped on:</strong> {scraped_at_formatted}
                    </div>
                    
                    <div class="btn-container">
                        <a href="{tender_data.get('source_url', '#')}" class="btn">View Tender Details</a>
                    </div>
                </div>
                <div class="footer">
                    <div class="social-links">
                        <span class="social-icon"><a href="https://x.com/growthpadEA" title="Twitter/X">𝕏</a></span>
                        <span class="social-icon"><a href="https://www.youtube.com/channel/UCDGqgoqam13s-e8BAw5xkCQ" title="YouTube">▶</a></span>
                        <span class="social-icon"><a href="https://ke.linkedin.com/company/growthpad-consulting" title="LinkedIn">in</a></span>
                        <span class="social-icon"><a href="https://www.facebook.com/growthpadconsulting/" title="Facebook">f</a></span>
                    </div>
                    <p>&copy; {current_year} Growthpad Consulting Group. All rights reserved.</p>
                    <p><a href="https://growthpad.co.ke">growthpad.co.ke</a> | <a href="mailto:strategic@growthpad.co.ke">Contact Us</a></p>
                </div>
            </div>
        </body>
        </html>
        """

        msg.attach(MIMEText(body, "html"))

        if EMAIL_SECURE:
            server = smtplib.SMTP_SSL(EMAIL_HOST, EMAIL_PORT)
        else:
            server = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT)
            server.starttls()

        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, recipient_email, msg.as_string())
        logger.info(f"Email sent successfully to {recipient_email} for tender: {tender_data.get('title', 'N/A')}")

        server.quit()

    except Exception as e:
        logger.error(f"Failed to send email to {recipient_email} for tender '{tender_data.get('title', 'N/A')}': {str(e)}")
        raise  # Optionally re-raise to allow caller to handle

def notify_open_tenders(tenders, task_id, recipient_emails=None):
    """
    Checks a list of tenders and sends an email for each open tender to the specified recipients.
    
    Args:
        tenders (list): List of tender dictionaries.
        task_id (int): The ID of the task that triggered this notification.
        recipient_emails (str): Comma-separated string of email addresses or a single email.
                               Defaults to DEFAULT_RECIPIENT_EMAIL if None or empty.
    """
    # Use DEFAULT_RECIPIENT_EMAIL if recipient_emails is None or empty
    if not recipient_emails:
        recipient_emails = DEFAULT_RECIPIENT_EMAIL
        logger.info(f"No recipient emails provided, using default: {DEFAULT_RECIPIENT_EMAIL}")

    # Split recipient_emails into a list of individual emails
    email_list = [email.strip() for email in recipient_emails.split(",") if email.strip()]
    
    if not email_list:
        logger.warning(f"No valid recipient emails for task {task_id}. Skipping notifications.")
        return

    for tender in tenders:
        if tender.get("status") == "open":
            logger.info(f"Found open tender for task {task_id}: {tender.get('title', 'N/A')}")
            for email in email_list:
                try:
                    send_open_tender_email(tender, email)
                except ValueError as e:
                    logger.error(f"Skipping email to {email}: {str(e)}")
                except Exception as e:
                    logger.error(f"Failed to send email to {email} for task {task_id}: {str(e)}")