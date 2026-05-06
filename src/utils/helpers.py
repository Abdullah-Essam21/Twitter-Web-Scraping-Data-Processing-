import re

def sanitize_folder_name(name):
    """Removes special characters to make a valid folder name."""
    if not name:
        return "unnamed_session"
    return re.sub(r'[^\w\s-]', '', name).strip().replace(' ', '_')

def format_timestamp():
    """Returns a standardized timestamp string."""
    import datetime
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
