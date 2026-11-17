<img src="assets/icon.svg" width="64" height="64" alt="">

# Strata

A small LSM-tree key-value database built from scratch in Python.

**Stage 1** — Write-Ahead Log + in-memory table + crash recovery.

## Quick start

```bash
from strata import Strata

with Strata("my_data") as db:
    db.put("hello", "world")
    print(db.get("hello"))   # "world"
    db.delete("hello")
    print(db.get("hello"))   # None
```

## Running the tests

```bash
python3 -m pytest tests/test_engine.py -v
python3 tests/crash_test.py
```
