from datetime import datetime
from urllib.parse import urlparse


def is_url_valid(url, url_type):
    if not url or url.strip() == "":
        return False, f"The {url_type} must be populated"

    parsed_url = urlparse(url)
    # is  url scheme valid i.e start with http:// or https://
    if parsed_url.scheme not in ["http", "https"] or not parsed_url.scheme:
        return False, f"The {url_type} must start with 'http://' or 'https://'"

    # does url have domain
    if not parsed_url.netloc:
        return False, f"The {url_type} must have a domain"

    # ensure domain has correct format
    if "." not in parsed_url.netloc:
        return (
            False,
            f"The {url_type} must have a valid domain with a top-level domain (e.g., '.gov.uk', '.com')",
        )

    return True, ""


def is_date_valid(date, date_type):
    if len(date) == 0:
        return False, "Date is blank"
    try:
        date = datetime.strptime(date, "%Y-%m-%d").date()
    # need to catch ValueError here otherwise datetime will raise it's own error, not the clear format we want
    except ValueError:
        return False, f"{date_type} {date} must be format YYYY-MM-DD"

    if date > datetime.today().date():
        return False, f"The {date_type} {date} cannot be in the future"

    return True, ""
