import pytest


def pytest_addoption(parser):
    parser.addoption("--core-url", action="store", default="localhost:6565")
    parser.addoption("--serving-url", action="store", default="localhost:6566")
    parser.addoption("--job-service-url", action="store", default="localhost:6568")
    parser.addoption("--kafka-brokers", action="store", default="localhost:9092")

    parser.addoption("--env", action="store", help="k8s", default="k8s")
    parser.addoption("--staging-path", action="store")
    parser.addoption("--k8s-namespace", action="store", default="sparkop-e2e")
    parser.addoption("--bq-project", action="store")
    parser.addoption("--feast-project", action="store", default="default")
    parser.addoption("--enable-auth", action="store_true")
    parser.addoption(
        "--scheduled-streaming-job",
        action="store_true",
        help="When set tests won't manually start streaming jobs,"
        " instead jobservice's loop is responsible for that",
    )


def pytest_runtest_setup(item):
    env_names = [mark.args[0] for mark in item.iter_markers(name="env")]
    if env_names:
        if item.config.getoption("env") not in env_names:
            pytest.skip(f"test requires env in {env_names}")


from .fixtures.client import *  # noqa
from .fixtures.data import *  # noqa
from .fixtures.external_services import *  # noqa
