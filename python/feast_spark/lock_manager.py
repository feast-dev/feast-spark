"""
Classes to manage distributed locks
"""
import enum
import logging
import secrets
import time

import redis
from redis.exceptions import ConnectionError

# retries for acquiring lock
LOCK_ACQUIRE_RETRIES = 3
# wait between retries
LOCK_ACQUIRE_WAIT = 1
LOCK_KEY_PREFIX = "lock"

logger = logging.getLogger(__name__)


class JobOperation(enum.Enum):
    """
    Enum for job operations
    """

    START = "st"
    CANCEL = "cn"


class JobOperationLock:
    """
    Lock for starting and cancelling spark ingestion jobs.
    Implemented as a context manager to automatically release lock after operation.

    Usage:
    with JobOperationLock(job_hash, <start/cancel>):
        client.start_stream_to_online_ingestion(feature_table, [], project=project)
    """

    def __init__(
        self,
        redis_host: str,
        redis_port: int,
        lock_expiry: int,
        job_hash: str,
        operation: JobOperation = JobOperation.START,
    ):
        """
        Init method, initialized redis key for the lock
        Args:
            redis_host: host to redis instance to store locks
            redis_port: port to redis instance to store locks
            lock_expiry: time in seconds for auto releasing lock
            job_hash: job hash string for the job which needs to be operated upon
            operation: operation to be performed <START/CANCEL>
        """
        self._redis = redis.Redis(host=redis_host, port=redis_port)
        self._lock_expiry = lock_expiry
        self._lock_key = f"{LOCK_KEY_PREFIX}_{operation.value}_{job_hash}"
        self._lock_value = secrets.token_hex(nbytes=8)

    def __enter__(self):
        """
        Context manager method for setup - acquire lock

        lock_key is a combination of a prefix, job hash and operation(start/cancel)

        lock_value is a randomly generated 8 byte hexadecimal, this is to ensure
        that lock can be deleted only by the agent who created it

        NX option is used only set the key if it does not already exist,
        this will ensure that locks are not overwritten

        EX option is used to set the specified expire time to release the lock automatically after TTL
        """
        # Retry acquiring lock on connection failures
        retry_attempts = 0
        while retry_attempts < LOCK_ACQUIRE_RETRIES:
            try:
                if self._redis.set(
                    name=self._lock_key,
                    value=self._lock_value,
                    nx=True,
                    ex=self._lock_expiry,
                ):
                    return self._lock_value
                else:
                    logger.info(f"lock not available: {self._lock_key}")
                    return False
            except ConnectionError:
                # wait before attempting to retry
                logger.warning(
                    f"connection error while acquiring lock: {self._lock_key}"
                )
                time.sleep(LOCK_ACQUIRE_WAIT)
                retry_attempts += 1
        logger.warning(f"Can't acquire lock, backing off: {self._lock_key}")
        return False

    def __exit__(self, *args, **kwargs):
        """
        context manager method for teardown - release lock

        safe release - delete lock key only if value exists and is same as set by this object
        otherwise rely on auto-release on expiry
        """
        try:
            lock_value = self._redis.get(self._lock_key)
            if lock_value and lock_value.decode() == self._lock_value:
                self._redis.delete(self._lock_key)
        except ConnectionError:
            logger.warning(
                f"connection error while deleting lock: {self._lock_key}."
                f"rely on auto-release after {self._lock_expiry} seconds"
            )
