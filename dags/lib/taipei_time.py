import datetime


def taipei_now():
    """台灣時間(UTC+8)的 naive datetime；容器時區為 UTC，所以用 now()+8h。"""
    return datetime.datetime.now() + datetime.timedelta(hours=8)
