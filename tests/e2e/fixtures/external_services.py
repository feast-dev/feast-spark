import pytest

__all__ = (
    "feast_core",
    "feast_serving",
    "kafka_server",
    "enable_auth",
    "feast_jobservice",
)


@pytest.fixture(scope="session")
def feast_core(pytestconfig):
    host, port = pytestconfig.getoption("core_url").split(":")
    return host, port


@pytest.fixture(scope="session")
def feast_serving(pytestconfig):
    host, port = pytestconfig.getoption("serving_url").split(":")
    return host, port


@pytest.fixture(scope="session")
def kafka_server(pytestconfig):
    host, port = pytestconfig.getoption("kafka_brokers").split(":")
    return host, port


@pytest.fixture(scope="session")
def enable_auth():
    return False


@pytest.fixture(scope="session")
def feast_jobservice(pytestconfig):
    host, port = pytestconfig.getoption("job_service_url").split(":")
    return host, port
