import os
import uuid
from typing import Tuple

import pytest

from feast import Client
from feast_spark import Client as SparkClient


@pytest.fixture
def feast_client(
    pytestconfig,
    feast_core: Tuple[str, int],
    feast_serving: Tuple[str, int],
    local_staging_path,
    feast_jobservice: Tuple[str, int],
    enable_auth,
):
    c = Client(
        core_url=f"{feast_core[0]}:{feast_core[1]}",
        serving_url=f"{feast_serving[0]}:{feast_serving[1]}",
        job_service_url=f"{feast_jobservice[0]}:{feast_jobservice[1]}",
        historical_feature_output_location=os.path.join(
            local_staging_path, "historical_output"
        ),
        spark_staging_location=os.path.join(local_staging_path, "k8s"),
        enable_auth=pytestconfig.getoption("enable_auth"),
    )

    c.set_project(pytestconfig.getoption("feast_project"))
    return c


@pytest.fixture
def feast_spark_client(feast_client: Client) -> SparkClient:
    return SparkClient(feast_client)


@pytest.fixture(scope="session")
def global_staging_path(pytestconfig):
    staging_path = pytestconfig.getoption("staging_path")
    return os.path.join(staging_path, str(uuid.uuid4()))


@pytest.fixture(scope="function")
def local_staging_path(global_staging_path):
    return os.path.join(global_staging_path, str(uuid.uuid4()))
