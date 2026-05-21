# target-snowflake — Python 3.9 → 3.11 Regression Tests

Before/after parity test for the Python 3.9 → 3.11 migration.

## Strategy

1. Feed a fixed Singer JSONL corpus through target-snowflake.
2. Snowflake connection is mocked — all SQL is captured, no real account needed.
3. Compare captured SQL and STATE output between Python 3.9 (baseline) and 3.11 (current).

## Usage

```bash
cd target-snowflake

# Capture baseline (master + Python 3.9)
git checkout master
docker run --rm -v "$(pwd)":/target -w /target python:3.9-slim \
  bash -c 'apt-get update -qq && apt-get install -y -qq git > /dev/null 2>&1 && pip install -e . -q 2>/dev/null && python tests/regression/capture.py --output baseline'

# Capture current (migration branch + Python 3.11)
git checkout python-311-migration
docker run --rm -v "$(pwd)":/target -w /target python:3.11-slim \
  bash -c 'apt-get update -qq && apt-get install -y -qq git > /dev/null 2>&1 && pip install -e . -q 2>/dev/null && python tests/regression/capture.py --output current'

# Compare
python3 -m pytest tests/regression/test_regression.py -v
```
