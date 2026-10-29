import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(scope="session")
def spark():
    from pipeline.spark.session import spark_session

    s = spark_session("tests")  # local, no object store
    yield s
    s.stop()
