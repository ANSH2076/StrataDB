"""Strata — a small LSM-tree key-value database.

Usage:
    from strata import Strata

    with Strata("my_data") as db:
        db.put("hello", "world")
        print(db.get("hello"))
"""

from strata.engine import Strata

__all__ = ["Strata"]
