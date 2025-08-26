import pytest
from fastapi.testclient import TestClient

from aw_connector_example.main import app


@pytest.fixture(scope='session')
def app_client():
    """ """
    yield TestClient(app=app)
