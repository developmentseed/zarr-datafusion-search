"""Pytest fixtures for zarr-datafusion tests."""
import pytest

from .fixtures import create_test_icechunk_data, create_test_zarr_data


@pytest.fixture
def zarr_store(tmp_path):
    """Create a temporary zarr store with test data.

    Yields the path to the zarr store, which is automatically cleaned up after the test.
    """
    store_path = tmp_path / "zarr_store.zarr"
    create_test_zarr_data(store_path)
    yield store_path


@pytest.fixture
def icechunk_store(tmp_path):
    """Create a temporary icechunk store with test data.

    Yields the path to the icechunk store, which is automatically cleaned up
    after the test.
    """
    store_path = tmp_path / "icechunk"
    create_test_icechunk_data(store_path)
    yield store_path


@pytest.fixture(scope="session")
def session_zarr_store(tmp_path_factory):
    """Create a session-scoped zarr store with test data.

    This fixture is created once per test session and shared across all tests.
    Useful for read-only tests that don't modify the data.
    """
    store_path = tmp_path_factory.mktemp("data") / "zarr_store.zarr"
    create_test_zarr_data(store_path)
    yield store_path


@pytest.fixture(scope="session")
def session_icechunk_store(tmp_path_factory):
    """Create a session-scoped icechunk store with test data.

    This fixture is created once per test session and shared across all tests.
    Useful for read-only tests that don't modify the data.
    """
    store_path = tmp_path_factory.mktemp("data") / "icechunk"
    create_test_icechunk_data(store_path)
    yield store_path
