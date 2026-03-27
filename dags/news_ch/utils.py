import re
import datetime
from urllib.parse import urlparse

_URL_RE = re.compile(
    r'^(?:http|https)://'
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'
    r'localhost|'
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
    r'(?::\d+)?'
    r'(?:/?|[/?]\S+)$', re.IGNORECASE)


def is_valid_url(url):
    if not url:
        return False
    if _URL_RE.match(url) is None:
        return False
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False


def clean_text(text, special_sign='"'):
    return re.sub(string=text, pattern=special_sign, repl='')


def clean_time_format(time_format):
    """Standardize to '%Y-%m-%d %H:%M:%S'; return None for invalid input."""
    time_format = str(time_format).replace('/', '-')
    time_format = re.sub(string=time_format, pattern='  ', repl=' ')

    if time_format[0] == '0':
        return None

    try:
        datetime.datetime.strptime(time_format, '%Y-%m-%d %H:%M:%S')
        return time_format
    except ValueError:
        try:
            dt = datetime.datetime.strptime(time_format, '%Y-%m-%d %H:%M')
            return dt.strftime('%Y-%m-%d %H:%M') + ':00'
        except ValueError:
            return None
