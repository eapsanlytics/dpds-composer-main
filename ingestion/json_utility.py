"""JSON related functions"""
import re

def sanitize(value, is_value=True):
    """Remove invalid BigQuery column name characters from the JSON keys"""
    if isinstance(value, dict):
        value = {sanitize(k,False):sanitize(v,True) for k, v in value.items()}
    elif isinstance(value, list):
        value = [sanitize(v, True) for v in value]
    elif isinstance(value, str):
        if not is_value:
            value = re.sub(r"[^A-Za-z0-9_]", "_", value)
    return value
