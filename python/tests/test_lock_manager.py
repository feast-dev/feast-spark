from unittest.mock import patch

import pytest

from feast_spark.lock_manager import JobOperation, JobOperationLock

job_hash = "dummy_hash"


class MockRedis:
    def __init__(self, cache=dict()):
        self.cache = cache

    def get(self, name):
        if name in self.cache:
            return self.cache[name]
        return None

    def set(self, name, value, *args, **kwargs):
        if name not in self.cache:
            self.cache[name] = value.encode("utf-8")
            return "OK"

    def delete(self, name):
        if name in self.cache:
            self.cache.pop(name)
        return None


@pytest.fixture
def lock_config():
    return {"redis_host": "localhost", "redis_port": 0, "lock_expiry": 5}


@patch("redis.Redis")
def test_lock_manager_context(mock_redis, lock_config):
    mock_redis_connection = MockRedis()
    mock_redis.return_value = mock_redis_connection
    with JobOperationLock(
        job_hash=job_hash, operation=JobOperation.START, **lock_config
    ) as lock:
        # test lock acquired
        assert lock
        # verify lock key in cache
        assert (
            f"lock_{JobOperation.START.value}_{job_hash}" in mock_redis_connection.cache
        )
    # verify release
    assert (
        f"lock_{JobOperation.START.value}_{job_hash}" not in mock_redis_connection.cache
    )


@patch("redis.Redis")
def test_lock_manager_lock_not_available(mock_redis, lock_config):
    cache = {"lock_st_dummy_hash": b"127a32aaf729dc87"}
    mock_redis_connection = MockRedis(cache)
    mock_redis.return_value = mock_redis_connection
    with JobOperationLock(
        job_hash=job_hash, operation=JobOperation.START, **lock_config
    ) as lock:
        # test lock not acquired
        assert not lock


def test_lock_manager_connection_error(lock_config):
    with JobOperationLock(
        job_hash=job_hash, operation=JobOperation.START, **lock_config
    ) as lock:
        # test lock not acquired
        assert not lock
