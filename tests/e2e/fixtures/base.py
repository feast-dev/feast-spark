import xml.etree.ElementTree as ET
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def java_project_root():
    # This file is %root%/tests/e2e/fixtures/base.py
    return Path(__file__).parent.parent.parent.parent / "deps" / "feast"


@pytest.fixture(scope="session")
def project_root():
    # This file is %root%/tests/e2e/fixtures/base.py
    return Path(__file__).parent.parent.parent.parent


@pytest.fixture(scope="session")
def project_version(pytestconfig, java_project_root):
    if pytestconfig.getoption("feast_version"):
        return pytestconfig.getoption("feast_version")

    pom_xml = ET.parse(java_project_root / "pom.xml")
    root = pom_xml.getroot()
    return root.find(".properties/revision").text
