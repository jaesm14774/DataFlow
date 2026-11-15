"""Value coercion helpers shared by the source_extension modules."""

from __future__ import annotations

import datetime as dt
import math
from typing import Any


def finite_float(value: Any) -> float | None:
    """float(value) when it parses to a finite number, otherwise None."""

    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def as_date(value: Any) -> dt.date:
    """Date keys are dt.date from MySQL but ISO strings elsewhere; raises on garbage."""

    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])
