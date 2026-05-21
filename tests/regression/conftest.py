"""Shared configuration for target-snowflake regression tests."""
import pytest

def pytest_configure(config):
    config.addinivalue_line("markers", "regression: target-snowflake 3.9→3.11 migration regression tests")
