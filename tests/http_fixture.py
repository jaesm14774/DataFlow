"""Shared helper for building canned ``requests.Response`` objects in tests."""

import requests


def make_response(body, status=200, content_type="application/json", encoding="utf-8"):
    item = requests.Response()
    item.status_code = status
    item.encoding = encoding
    item.headers["Content-Length"] = str(len(body))
    item.headers["Content-Type"] = content_type
    item._content = body
    item._content_consumed = True
    return item
